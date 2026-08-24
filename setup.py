from setuptools import setup, find_packages

setup(
    name="banking-upi-pipeline",
    version="1.0.0",
    description="End-to-end UPI banking data pipeline (Snowflake-native ELT)",
    author="Data Engineering",
    python_requires=">=3.11",
    packages=find_packages(
        exclude=["tests", "tests.*", "data", "docker", "dbt", "dags"]
    ),
    install_requires=[
        "pandas>=2.1.0",
        "pyarrow>=14.0.0",
        "requests>=2.31.0",
        "python-dotenv>=1.0.0",
    ],
    extras_require={
        "dev": [
            "black>=23.7.0",
            "ruff>=0.0.285",
        ],
        "api": [
            "fastapi>=0.110.0",
            "uvicorn[standard]>=0.27.0",
        ],
        "snowflake": [
            "snowflake-connector-python>=3.6.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "upi-ingest-csv=ingestion.csv_reader:main",
            "upi-ingest-api=ingestion.api_client:main",
            "upi-generate-data=data.mock_data_generator:main",
        ],
    },
)
