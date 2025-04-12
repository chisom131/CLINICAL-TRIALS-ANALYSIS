# 🧬 Clinical Trial Data Analysis with Databricks

This project analyzes clinical trial data to extract meaningful insights using Apache Spark on Databricks. The aim is to explore trial trends, study characteristics, and sponsor activities to support data-driven healthcare decisions.

## 📌 Project Objective
To perform exploratory data analysis (EDA) on a large-scale clinical trial dataset using distributed processing with Apache Spark, and generate insights on study frequency, common conditions, and sponsor participation over time.

## 🛠️ Tools & Technologies
- **Platform**: Databricks (Community Edition)
- **Languages**: Python, SQL (Spark SQL)
- **Libraries**: PySpark, Matplotlib
- **Data Abstraction**: RDDs, DataFrames

## 📊 Dataset
- **Source**: Clinical Trial 2023 dataset
- **Size**: Large-scale dataset (thousands of records)
- **Features**: Study type, sponsor, condition, status, start/completion date

## 🧹 Data Preprocessing
- Cleaned missing values and inconsistencies in trial records
- Standardized formats across dates and sponsor names
- Filtered data for ongoing, completed, and withdrawn trials

## 🔍 Exploratory Data Analysis (EDA)
- Identified the most common conditions under study
- Analyzed study types (interventional, observational, etc.)
- Tracked trial volume trends over time
- Evaluated top sponsors and completion rates

## 📈 Visualizations
- Bar charts of study counts by condition and sponsor
- Line plots showing trial start/completion trends over time
- SQL-based summaries displayed using Databricks visual tools

## 🧠 Key Insights
- Cardiovascular and oncological conditions had the highest number of studies
- Interventional studies made up the majority of trials
- A few key sponsors contributed significantly to completed studies
- Trial activity showed peaks in specific years correlating with public health focus

## 📂 Project Structure
```
📁 clinical-trial-analysis/
│
├── notebooks/             # Databricks notebooks for data processing and EDA
├── data/                  # Raw dataset and cleaned data files
├── outputs/               # Visualizations and summary results
└── README.md              # Project documentation
```

## 🧠 Future Work
- Apply topic modeling or NLP on study abstracts to extract themes
- Explore machine learning to predict trial success or delay
- Integrate external data like FDA approvals or patient outcomes

## 👤 Author
**Chisom Onumaegbu**  
MSc Data Science Candidate | University of Salford  
[GitHub](https://github.com/chisom131) | [LinkedIn](https://www.linkedin.com/in/chisom-onumaegbu-a85757171)
