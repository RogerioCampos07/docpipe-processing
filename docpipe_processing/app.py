from fastapi import FastAPI

app = FastAPI(title='DocPipe Processing')


@app.get('/health')
def health() -> dict[str, str]:
    return {'status': 'ok'}
