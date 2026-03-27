# Notebooks

```
GET    /api/v1/notebooks
POST   /api/v1/notebooks
GET    /api/v1/notebooks/{id}
PUT    /api/v1/notebooks/{id}
DELETE /api/v1/notebooks/{id}

POST   /api/v1/notebooks/{id}/sections
PUT    /api/v1/notebooks/{id}/sections/{section_id}
DELETE /api/v1/notebooks/{id}/sections/{section_id}
POST   /api/v1/notebooks/{id}/sections/{section_id}/execute
POST   /api/v1/notebooks/{id}/sections/{section_id}/summarize
POST   /api/v1/notebooks/{id}/sections/reorder
GET    /api/v1/notebooks/ai-status
POST   /api/v1/notebooks/generate-from-comments
POST   /api/v1/notebooks/import
GET    /api/v1/notebooks/{id}/export
```

`POST /api/v1/notebooks/generate-from-comments` accepts `{"tag": "...", "attack_chain": false}` and creates a notebook from all comments with that tag. Set `attack_chain` to `true` to generate a MITRE ATT&CK tactic breakdown instead of a plain AI summary. If a notebook named "Notebook: {tag}" already exists in the fractal, it is replaced.
