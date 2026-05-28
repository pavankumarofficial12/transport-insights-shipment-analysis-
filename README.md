# Transport Insights - Shipment Analysis API

An intelligent **real-time shipment analytics API** designed for transport companies. It provides comprehensive insights into vehicle performance, shipment metrics, income, and delivery status across different time periods (week, month, year).

---

## ✨ Features

- **Multi-period Analysis**: Supports week, month, and year-wise insights
- **Vehicle-wise Breakdown**: Detailed performance metrics per vehicle
- **Real-time Data Aggregation**: Total distance, income, delivered/pending/in-transit shipments
- **High Performance**: Redis caching (10 min TTL) + Async processing
- **Rate Limiting**: 5 requests per minute per IP
- **Error Handling & Logging**: Comprehensive logging to file and console

---

## 🛠 Tech Stack

- **Framework**: FastAPI
- **Async HTTP Client**: httpx
- **Caching**: Redis (aioredis)
- **Rate Limiting**: SlowAPI
- **Database/Backend**: External API integration (REST)

---

## 🚀 Setup Instructions

### 1. Clone the Repository
```bash
git clone https://github.com/
cd transport-insights-shipment-analysis
