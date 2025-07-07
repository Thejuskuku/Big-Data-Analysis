import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

# --- 1. Data Setup: Create a Sample DataFrame with Issues ---
print("--- 1. Data Setup: Original DataFrame ---")
data = {
    'UserID': [101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 101], # Added a duplicate
    'Name': ['Alice Smith', 'Bob Johnson', 'Charlie Brown', 'Diana Prince', 'Eve Adams',
             'Frank White', 'Grace Lee', 'Heidi Miller', 'Ivan Petrov', 'Judy Clark',
             'Alice Smith', 'Liam Neeson', 'Olivia Davis', 'Peter Jones', 'Quinn Wilson', 'Alice Smith'],
    'Age': [25, 30, np.nan, 45, 22, 55, 30, 28, 40, np.nan, 25, 150, 33, 29, 38, 25], # Missing values, outlier
    'City': ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix',
             'New York', 'Dallas', 'PhOENIX', 'San Francisco', 'miami',
             'New York', 'London', 'chicago ', 'Houston', 'Miami', 'New York'], # Inconsistent casing, trailing space
    'Income': [50000, 60000, 75000, 90000, 48000, 120000, 65000, 52000, 80000, 70000,
               50000, 70000, 78000, 85000, 62000, 50000],
    'Enrollment_Date': ['2023-01-15', '2022-03-20', '2023-05-10', '2021-11-25', '2024-02-01',
                        '2022-07-01', '2023-09-12', '2023/04/05', '2022-08-30', '2024-01-01',
                        '2023-01-15', '2023-10-01', '2022-06-18', '2023-03-22', '2024-05-01', '2023-01-15'],
    'Product_Category': ['Electronics', 'Home Goods', 'Clothing', 'Electronics', 'Books',
                         'Electronics', 'Home Goods', 'Electronics', 'Clothing', 'Books',
                         'Electronics', 'Electronics', 'Home Goods', 'Clothing', 'Books', 'Electronics'],
    'Email': ['alice@email.com', 'bob@email.com', 'charlie@email.com', 'diana@email.com', 'eve@email.com',
              'frank@email.com', 'grace@email.com', 'heidi@email.com', 'ivan@email.com', 'judy@email.com',
              'alice@email.com', 'liam@email.com', 'olivia@email.com', 'peter@email.com', 'quinn@email.com', 'alice@email.com'],
    'Rating': [4.5, 3.8, 5.0, 4.2, 3.5, 4.9, 4.0, 3.7, 4.8, 3.9, 4.5, 4.1, 4.3, 3.6, 4.7, 4.5]
}

df = pd.DataFrame(data)
print("Initial DataFrame:")
print(df)
print("\nMissing values before cleaning:")
print(df.isnull().sum())
print("\n" + "="*70 + "\n")

# --- 2. Data Cleaning Process ---

# Create a working copy to apply changes
df_cleaned = df.copy()

# --- 2.1 Handling Missing Values ---
print("--- 2.1 Handling Missing Values ---")

# Detect missing values
print("\nMissing values before imputation:")
print(df_cleaned.isnull().sum())

# Impute 'Age' with its median (robust to outliers)
median_age = df_cleaned['Age'].median()
df_cleaned['Age'].fillna(median_age, inplace=True)
print(f"\n'Age' column imputed with median: {median_age}")

# For Product_Category, let's simulate a missing value and impute with mode
# (Although in this specific dataset, there were no initial NaNs for it)
# df_cleaned.loc[2, 'Product_Category'] = np.nan # Uncomment to test
mode_product_category = df_cleaned['Product_Category'].mode()[0]
df_cleaned['Product_Category'].fillna(mode_product_category, inplace=True)
print(f"'Product_Category' column imputed with mode: '{mode_product_category}'")

print("\nMissing values after imputation:")
print(df_cleaned.isnull().sum())
print("\n" + "="*70 + "\n")

# --- 2.2 Handling Outliers ---
print("--- 2.2 Handling Outliers ---")

# Visualize Age distribution before handling outliers
plt.figure(figsize=(8, 4))
sns.boxplot(x=df_cleaned['Age'])
plt.title('Age Distribution Before Outlier Handling')
plt.xlabel('Age')
plt.show()

# Detect outliers in 'Age' using IQR method
Q1_age = df_cleaned['Age'].quantile(0.25)
Q3_age = df_cleaned['Age'].quantile(0.75)
IQR_age = Q3_age - Q1_age
lower_bound_age = Q1_age - 1.5 * IQR_age
upper_bound_age = Q3_age + 1.5 * IQR_age

print(f"Age Q1: {Q1_age}, Q3: {Q3_age}, IQR: {IQR_age}")
print(f"Age Lower Bound (IQR): {lower_bound_age}")
print(f"Age Upper Bound (IQR): {upper_bound_age}")

outliers_age_detected = df_cleaned[(df_cleaned['Age'] < lower_bound_age) | (df_cleaned['Age'] > upper_bound_age)]
print("\nOutliers in 'Age' column (detected by IQR):")
print(outliers_age_detected[['UserID', 'Age']])

# Cap/Winsorize outliers in 'Age'
# Replace values above upper_bound with upper_bound, and below lower_bound with lower_bound
df_cleaned['Age'] = np.where(df_cleaned['Age'] > upper_bound_age, upper_bound_age, df_cleaned['Age'])
df_cleaned['Age'] = np.where(df_cleaned['Age'] < lower_bound_age, lower_bound_age, df_cleaned['Age'])

print("\n'Age' column after capping outliers:")
print(df_cleaned[['UserID', 'Age']].head(12)) # Check a few rows including the former outlier

# Visualize Age distribution after handling outliers
plt.figure(figsize=(8, 4))
sns.boxplot(x=df_cleaned['Age'])
plt.title('Age Distribution After Outlier Handling (Capped)')
plt.xlabel('Age')
plt.show()
print("\n" + "="*70 + "\n")

# --- 2.3 Removing Duplicates ---
print("--- 2.3 Removing Duplicates ---")

# Detect duplicates (based on all columns by default, or specific key columns)
print("\nDuplicate rows before removal (showing all duplicate instances):")
# Use subset if you only care about duplicates in specific columns, e.g., subset=['UserID', 'Email']
print(df_cleaned[df_cleaned.duplicated(keep=False)])

# Remove duplicate rows, keeping the first occurrence
print(f"\nDataFrame shape before removing duplicates: {df_cleaned.shape}")
df_cleaned.drop_duplicates(inplace=True)
print(f"DataFrame shape after removing duplicates: {df_cleaned.shape}")

print("\nDataFrame after removing duplicates:")
print(df_cleaned)
print("\n" + "="*70 + "\n")

# --- 2.4 Correcting Errors / Inconsistencies ---
print("--- 2.4 Correcting Errors / Inconsistencies ---")

# Inconsistencies in 'City' column (casing, leading/trailing spaces)
print("\nUnique values in 'City' before cleaning:")
print(df_cleaned['City'].unique())

df_cleaned['City'] = df_cleaned['City'].str.lower().str.strip()
print("\nUnique values in 'City' after standardizing (lowercase and stripped):")
print(df_cleaned['City'].unique())

# Inconsistencies in 'Enrollment_Date' (mixed formats, incorrect data type)
print("\n'Enrollment_Date' original data types:")
print(df_cleaned['Enrollment_Date'].dtype)
print("\nSample 'Enrollment_Date' values before conversion:")
print(df_cleaned['Enrollment_Date'].head())

# Convert to datetime, coercing errors
df_cleaned['Enrollment_Date'] = pd.to_datetime(df_cleaned['Enrollment_Date'], errors='coerce')

print("\n'Enrollment_Date' data type after conversion:")
print(df_cleaned['Enrollment_Date'].dtype)
print("\nSample 'Enrollment_Date' values after conversion:")
print(df_cleaned['Enrollment_Date'].head())

# Check if any dates became NaT due to 'coerce'
if df_cleaned['Enrollment_Date'].isnull().any():
    print("\nWarning: Some Enrollment_Date values were unparseable and converted to NaT.")
    # You might want to impute these NaT values if present
    # e.g., df_cleaned['Enrollment_Date'].fillna(df_cleaned['Enrollment_Date'].mode()[0], inplace=True)

print("\n" + "="*70 + "\n")

# --- 3. Final Review of Cleaned DataFrame ---
print("--- 3. Final Cleaned DataFrame ---")
print("\nInfo of the Cleaned DataFrame:")
df_cleaned.info()

print("\nHead of the Cleaned DataFrame:")
print(df_cleaned.head(10))

print("\nMissing values in the Cleaned DataFrame:")
print(df_cleaned.isnull().sum())

# Display value counts for cleaned categorical columns
print("\nValue counts for 'City' after cleaning:")
print(df_cleaned['City'].value_counts())
print("\nValue counts for 'Product_Category' after cleaning:")
print(df_cleaned['Product_Category'].value_counts())
