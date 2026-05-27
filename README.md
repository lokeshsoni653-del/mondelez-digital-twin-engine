# 🏭 Mondelēz Hub Plant: AI-Powered Digital Twin & Telemetry Engine

## 📌 Project Overview
This repository contains a high-velocity **Digital Twin and IoT Telemetry Engine** rapidly prototyped for the Mondelēz Hub Plant operations (Cadbury Dairy Milk Main Line). Built entirely in Python without heavy external web frameworks, this system simulates, processes, and visualizes critical supply chain and manufacturing data in real time.

The architecture was engineered to demonstrate advanced **Business Intelligence (BI)** capabilities, predictive inventory tracking, and operational latency reduction for large-scale FMCG manufacturing.

## 🚀 Core Architecture
* **Live Telemetry Pipeline (`telemetry_pipeline.py`):** Simulates a continuous stream of IoT sensor data, tracking critical raw materials (Cocoa/Sugar inventory), conveyor velocity, and processing temperatures at a configurable tick rate.
* **Custom WebSocket Server (`ws_server.py`):** A lightweight, pure Python stdlib implementation of an RFC 6455 compliant WebSocket server. It broadcasts the high-frequency telemetry JSON payload to connected clients without relying on external dependencies like Uvicorn or FastAPI.
* **Tactical BI Dashboard (`frontend/index.html`):** A dynamic, asynchronous frontend UI that catches the WebSocket stream and visualizes production KPIs. It includes automated threshold alerts (e.g., visual warnings when Cocoa reserves drop below critical operational levels).

## 🛠️ Tech Stack
* **Backend:** Python 3 (Standard Library), `asyncio`, `socket`, `threading`
* **Frontend:** HTML5, CSS3, JavaScript (Native WebSocket API)
* **Data Format:** JSON

## ⚙️ How to Run Locally
1. Clone the repository:
   ```bash
   git clone [https://github.com/Lokesh-soni/mondelez-digital-twin-engine.git](https://github.com/Lokesh-soni/mondelez-digital-twin-engine.git)
