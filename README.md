# 🛒 Australian Grocery Price Analysis

A comprehensive data pipeline for scraping, analysing, and tracking grocery prices from major Australian supermarkets (Coles and Woolworths) using Mage.ai.

## 📋 Overview

This project implements automated web scraping and data processing pipelines to collect grocery product information and prices from:
- **Coles** - Complete category hierarchy and product data
- **Woolworths** - Complete category hierarchy and product data

The system is built using **Mage.ai** as the orchestration platform with Docker containerisation for easy deployment.

## 🏗️ Architecture

### Data Pipeline Structure
- **Data Loaders**: Category scrapers (L1, L2, L3) and product crawlers for both retailers
- **Data Exporters**: BigQuery integration for data storage and analytics
- **Transformers**: Data processing and cleaning modules
- **Pipelines**: End-to-end orchestration with scheduled triggers

### Key Components
```
oz-grocery-price-analysis/
├── data_loaders/           # Web scraping modules
├── data_exporters/         # BigQuery export modules  
├── transformers/           # Data transformation logic
├── pipelines/              # Orchestration workflows
├── local_test/             # Standalone testing scripts
└── terraform/              # Infrastructure as Code
```

## 🚀 Features

- **Multi-level Category Scraping**: Hierarchical category collection (L1→L2→L3)
- **Product Data Extraction**: Comprehensive product information including prices, descriptions, and availability
- **Automated Scheduling**: Configurable pipeline triggers for regular data collection
- **Cloud Storage**: BigQuery integration for scalable data warehousing
- **Infrastructure as Code**: Terraform configurations for Google Cloud deployment
- **Docker Support**: Containerised deployment with Docker Compose

## 🔧 Tech Stack

- **Orchestration**: Mage.ai
- **Web Scraping**: Selenium WebDriver
- **Data Processing**: Pandas
- **Cloud Platform**: Google Cloud Platform
- **Database**: BigQuery
- **Infrastructure**: Terraform
- **Containerisation**: Docker & Docker Compose
- **Languages**: Python

## 🚀 Quick Start

### Prerequisites
- Docker and Docker Compose
- Google Cloud Platform account
- BigQuery dataset configured

### Local Development
1. **Clone the repository**
   ```bash
   git clone https://github.com/your-username/oz-grocery-price-analysis.git
   cd oz-grocery-price-analysis
   ```

2. **Set up environment variables**
   ```bash
   cp .env.example .env
   # Edit .env with your configuration
   ```

3. **Start the Mage.ai server**
   ```bash
   docker-compose up
   ```

4. **Access the Mage UI**
   - Open http://localhost:6789 in your browser
   - Navigate to pipelines to run data collection workflows

### Testing Individual Components
```bash
# Test Coles category scraper
python local_test/coles_category_crawler.py

# Test Woolworths category scraper  
python local_test/woolies_category_crawler.py

# Test product crawler
python local_test/coles_product_crawler.py
```

## 📊 Available Pipelines

### Category Data Collection
- `coles_end_to_end_crawler`: Complete Coles data pipeline
- `woolies_end_to_end_crawler`: Complete Woolworths data pipeline
- Individual category level pipelines (L1, L2, L3)

### Backfill Operations
- `coles_cat_and_product_backfill`: Historical data collection for Coles
- `woolies_cat_and_product_backfill`: Historical data collection for Woolworths
- `coles_product_backfill`: Product-specific backfill
- `woolies_product_backfill`: Product-specific backfill

## ☁️ Cloud Deployment

### Google Cloud Run Deployment
```bash
cd terraform
terraform init
terraform plan
terraform apply
```

The Terraform configuration deploys:
- Cloud Run service for the Mage.ai application
- Required IAM roles and permissions
- Network and security configurations

### Environment Variables Required
- `ENV`: Environment (dev/staging/prod)
- `GOOGLE_CLOUD_PROJECT`: GCP Project ID
- BigQuery credentials and dataset information

## 📈 Data Schema

### Category Data
- Category hierarchy (L1, L2, L3)
- Category names and IDs
- Navigation links and metadata

### Product Data
- Product names, descriptions, and prices
- Brand information
- Availability status
- Category associations
- Timestamps for price tracking

## 🔄 Scheduling

Pipelines include configurable triggers for automated execution:
- **Daily**: Product price updates
- **Weekly**: Category structure refresh
- **On-demand**: Backfill operations

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test locally using the local_test scripts
5. Submit a pull request

## 📝 License

This project is for educational and research purposes. Please respect the terms of service of the target websites.

## ⚠️ Disclaimer

This tool is designed for legitimate price monitoring and research purposes. Users are responsible for complying with website terms of service and applicable laws regarding web scraping.
