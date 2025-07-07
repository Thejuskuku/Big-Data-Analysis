How to run this code:

    Save: Save the code as a Python file (e.g., data_cleaning_example.py).

    Install Libraries: If you don't have them, install pandas, numpy, matplotlib, and seaborn:
    Bash

pip install pandas numpy matplotlib seaborn

Run: Execute the script from your terminal:
Bash

    python data_cleaning_example.py

Explanation of the Code Flow:

    Data Setup:

        A sample pd.DataFrame is created with intentional missing values (np.nan), an obvious outlier (150 in Age), duplicate rows, and inconsistent text/date formats.

        Initial df.isnull().sum() and df.info() are printed to show the raw state.

    Handling Missing Values:

        df.isnull().sum() is used to re-check missing values.

        Age column's missing values are filled using median(), which is good for numerical data that might have outliers.

        Product_Category's missing values (if any were there, we demonstrate the approach) are filled with mode(), suitable for categorical data.

    Handling Outliers:

        A box plot of Age is displayed before outlier handling for visual context.

        The IQR method is applied to Age to programmatically identify outliers.

        The identified outliers are then capped (winsorized) by replacing values beyond the bounds with the boundary values themselves. This helps mitigate their extreme influence without deleting data points.

        A box plot of Age is displayed after outlier handling to show the effect.

    Removing Duplicates:

        df.duplicated(keep=False) is used to show all duplicate rows before removal.

        df.drop_duplicates(inplace=True) is called to remove duplicate rows, keeping the first occurrence by default. The inplace=True modifies the DataFrame directly.

    Correcting Errors / Inconsistencies:

        City column: str.lower() and str.strip() are used in sequence to convert all city names to lowercase and remove any leading/trailing whitespace, ensuring consistency.

        Enrollment_Date column: pd.to_datetime() is used to convert the column to a proper datetime data type. errors='coerce' is crucial here; it tells pandas to put NaT (Not a Time) wherever it encounters a date string it cannot parse, allowing you to deal with those specific errors later if needed.

    Final Review:

        df_cleaned.info() provides a summary of data types and non-null counts after all operations.

        df_cleaned.head() shows the first few rows of the final cleaned DataFrame.

        df_cleaned.isnull().sum() confirms that most missing values have been handled.

        value_counts() is used on the City and Product_Category columns to show the standardized categories.
