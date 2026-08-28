package storage

import (
	"context"
	"database/sql"
	"fmt"
)

// StartUserTOTP stores a freshly generated secret without enrolling the user.
// Enrollment only counts once they prove they can produce a code, so an
// abandoned setup never locks anyone out of their account.
func (c *PostgresClient) StartUserTOTP(ctx context.Context, username, sealedSecret string) error {
	_, err := c.db.ExecContext(ctx, `
		UPDATE users
		SET totp_secret = $2, totp_enrolled_at = NULL, totp_last_counter = 0
		WHERE username = $1
	`, username, sealedSecret)
	if err != nil {
		return fmt.Errorf("failed to store TOTP secret: %w", err)
	}
	return nil
}

// ConfirmUserTOTP marks enrollment complete and spends the confirming step.
func (c *PostgresClient) ConfirmUserTOTP(ctx context.Context, username string, counter int64) error {
	_, err := c.db.ExecContext(ctx, `
		UPDATE users
		SET totp_enrolled_at = NOW(), totp_last_counter = $2
		WHERE username = $1
	`, username, counter)
	if err != nil {
		return fmt.Errorf("failed to confirm TOTP enrollment: %w", err)
	}
	return nil
}

// SpendTOTPCounter records a time step as used and reports whether this caller
// was the one to claim it. The comparison is in the UPDATE so that two requests
// racing with the same code cannot both succeed.
func (c *PostgresClient) SpendTOTPCounter(ctx context.Context, username string, counter int64) (bool, error) {
	res, err := c.db.ExecContext(ctx, `
		UPDATE users SET totp_last_counter = $2
		WHERE username = $1 AND totp_last_counter < $2
	`, username, counter)
	if err != nil {
		return false, fmt.Errorf("failed to record TOTP counter: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

// ClearUserTOTP removes enrollment and every recovery code for a user.
func (c *PostgresClient) ClearUserTOTP(ctx context.Context, username string) error {
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, `
		UPDATE users
		SET totp_secret = NULL, totp_enrolled_at = NULL, totp_last_counter = 0
		WHERE username = $1
	`, username); err != nil {
		return fmt.Errorf("failed to clear TOTP enrollment: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM user_recovery_codes WHERE username = $1`, username); err != nil {
		return fmt.Errorf("failed to clear recovery codes: %w", err)
	}
	return tx.Commit()
}

// ReplaceRecoveryCodes swaps a user's recovery codes for a new set. Issuing new
// codes always invalidates the old ones, so a leaked list cannot outlive it.
func (c *PostgresClient) ReplaceRecoveryCodes(ctx context.Context, username string, hashes []string) error {
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, `DELETE FROM user_recovery_codes WHERE username = $1`, username); err != nil {
		return fmt.Errorf("failed to clear recovery codes: %w", err)
	}
	for _, hash := range hashes {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO user_recovery_codes (username, code_hash) VALUES ($1, $2)`,
			username, hash,
		); err != nil {
			return fmt.Errorf("failed to store recovery code: %w", err)
		}
	}
	return tx.Commit()
}

// ConsumeRecoveryCode spends a code and reports whether it was valid and unused.
// The check lives in the UPDATE so one code cannot be redeemed twice.
func (c *PostgresClient) ConsumeRecoveryCode(ctx context.Context, username, hash string) (bool, error) {
	res, err := c.db.ExecContext(ctx, `
		UPDATE user_recovery_codes SET used_at = NOW()
		WHERE username = $1 AND code_hash = $2 AND used_at IS NULL
	`, username, hash)
	if err != nil {
		return false, fmt.Errorf("failed to consume recovery code: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

// CountUnusedRecoveryCodes reports how many codes a user has left.
func (c *PostgresClient) CountUnusedRecoveryCodes(ctx context.Context, username string) (int, error) {
	var n int
	err := c.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM user_recovery_codes
		WHERE username = $1 AND used_at IS NULL
	`, username).Scan(&n)
	if err != nil && err != sql.ErrNoRows {
		return 0, fmt.Errorf("failed to count recovery codes: %w", err)
	}
	return n, nil
}
