import os
import pandas as pd

def update_monthly_csv(df, date_column, base_dir='data_by_year'):
    # Convert the 'date_column' to datetime if it's not already
    df[date_column] = pd.to_datetime(df[date_column])
    
    # Extract year and month from the date column
    df['year'] = df[date_column].dt.year
    df['month'] = df[date_column].dt.month
    
    # Group the data by year and month
    grouped = df.groupby(['year', 'month'])

    for (year, month), group in grouped:
        # Create directory for the year if it doesn't exist
        year_dir = os.path.join(base_dir, str(year))
        os.makedirs(year_dir, exist_ok=True)
        
        # Define the file name for the month
        file_name = f'{month:02d}.csv'  # e.g., '01.csv' for January
        file_path = os.path.join(year_dir, file_name)
        
        if os.path.exists(file_path):
            # If the file exists, load the existing data
            existing_data = pd.read_csv(file_path)
            
            # Combine the existing data with the new data and drop duplicates
            combined_data = pd.concat([existing_data, group]).drop_duplicates()
            
            # Save the combined data back to the CSV
            combined_data.to_csv(file_path, index=False)
        else:
            # If the file doesn't exist, save the new data as the first entry
            group.to_csv(file_path, index=False)

    print("Files updated successfully.")

# Example usage:
# df is your DataFrame with new weekly data
# 'date' is the column in your DataFrame that contains the date information

# Load your newly scraped data into df
# df = pd.read_csv('your_new_data.csv') # Example

# Call the function to update the CSV files
update_monthly_csv(df, date_column='date')
