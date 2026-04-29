from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.routers import auth, admin_users, folders, cores, connects, similarity, sync, admin, public
from app.config import settings

app = FastAPI(title="Cosh 2.0 API", version="0.1.0")

_origins = [o.strip() for o in settings.cors_origins.split(",") if o.strip()]

app.add_middleware(
    CORSMiddleware,
    allow_origins=_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(auth.router)
app.include_router(admin_users.router)
app.include_router(folders.router)
app.include_router(cores.router)
app.include_router(connects.router)
app.include_router(similarity.router)
app.include_router(sync.router)
app.include_router(admin.router)
app.include_router(public.router)


@app.get("/")
async def root():
    return {"status": "Cosh 2.0 API is running"}


@app.get("/health")
async def health():
    return {"status": "ok"}
