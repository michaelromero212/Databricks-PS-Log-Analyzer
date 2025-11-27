.PHONY: dev-backend dev-frontend dev install

install:
	cd backend && pip install -r requirements.txt
	cd frontend && npm install

dev-backend:
	cd backend && uvicorn app.main:app --reload --port 8000

dev-frontend:
	cd frontend && npm run dev

dev:
	@echo "Starting backend and frontend concurrently..."
	@make -j 2 dev-backend dev-frontend

docker-up:
	docker-compose up --build

docker-down:
	docker-compose down
