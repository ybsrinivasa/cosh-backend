from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.routers import auth, admin_users, folders, cores

app = FastAPI(title="Cosh 2.0 API", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(auth.router)
app.include_router(admin_users.router)
app.include_router(folders.router)
app.include_router(cores.router)


@app.get("/")
async def root():
    return {"status": "Cosh 2.0 API is running"}


@app.get("/health")
async def health():
    return {"status": "ok"}
