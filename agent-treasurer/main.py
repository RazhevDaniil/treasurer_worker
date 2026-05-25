import uvicorn

from app.app import app
from app.core.config import settings


if __name__ == '__main__':
    uvicorn.run(
        app,
        host=settings.app_host,
        port=settings.app_port,
        reload=True
    )
