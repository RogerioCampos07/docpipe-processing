import os

import uvicorn

from docpipe_processing.observability import (
    configure_logging,
    shutdown_tracing,
)

if __name__ == '__main__':
    configure_logging()
    try:
        uvicorn.run(
            'docpipe_processing.app:app',
            host=os.getenv('PROCESSING_HOST', '127.0.0.1'),
            port=int(os.getenv('PROCESSING_PORT', '8000')),
            log_config=None,
            access_log=False,
        )
    finally:
        shutdown_tracing()
