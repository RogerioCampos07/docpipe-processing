import os

import uvicorn

if __name__ == '__main__':
    uvicorn.run(
        'docpipe_processing.app:app',
        host=os.getenv('PROCESSING_HOST', '127.0.0.1'),
        port=int(os.getenv('PROCESSING_PORT', '8000')),
    )
