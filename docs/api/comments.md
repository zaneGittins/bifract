# Comments

```
POST   /api/v1/comments
GET    /api/v1/comments/{id}
PUT    /api/v1/comments/{id}
DELETE /api/v1/comments/{id}

GET    /api/v1/logs/{log_id}/comments
DELETE /api/v1/logs/{log_id}/comments

GET    /api/v1/logs/commented
GET    /api/v1/comments/flat
GET    /api/v1/comments/tags

POST   /api/v1/comments/bulk-add-tag
POST   /api/v1/comments/bulk-remove-tag
POST   /api/v1/comments/bulk-delete
```

**Create comment:**

```json
{
  "log_id": "uuid",
  "content": "Confirmed malicious - escalating to IR team",
  "fractal_id": "uuid"
}
```
