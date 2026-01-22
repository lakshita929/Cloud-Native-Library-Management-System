Cloud-Native Library Management System
Overview

A cloud-native, microservices-based library management system built to demonstrate containerization, asynchronous communication, and Kubernetes deployment using modern cloud infrastructure practices.

Architecture

Microservices: User Service, Book Service, Borrow Service

Communication:

REST APIs (synchronous)

RabbitMQ for asynchronous borrow requests

Database: PostgreSQL (shared across services)

Containerization: Docker & Docker Compose

Orchestration: Kubernetes (Kompose-generated manifests)

Key Features

CRUD APIs for users and books

Event-driven borrow workflow with validation rules (max 5 books per user)

Decoupled services with message-based processing

Fully containerized and Kubernetes-deployed backend

Tech Stack

Python (Flask)

Docker, Docker Compose

RabbitMQ

PostgreSQL

Kubernetes, Kompose

Deployment

Docker images published to Docker Hub

Kubernetes manifests generated via Kompose

Services exposed and tested using kubectl port-forward
