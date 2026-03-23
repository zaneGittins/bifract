# Comments

```
POST   /api/v1/comments
GET    /api/v1/comments/{id}
PUT    /api/v1/comments/{id}
DELETE /api/v1/comments/{id}

GET    /api/v1/logs/{log_id}/comments
DELETE /api/v1/logs/{log_id}/comments

GET    /api/v1/logs/commented
```

**Create comment:**

```json
{
  "log_id": "uuid",
  "content": "Confirmed malicious - escalating to IR team",
  "fractal_id": "uuid"
}
```
