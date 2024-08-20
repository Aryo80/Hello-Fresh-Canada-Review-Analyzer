import re
import os
import pandas as pd
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem import WordNetLemmatizer
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer
from prefect import task
# Set the path for NLTK data
custom_nltk_data_path = '/workspaces/Hello-Fresh-Canada-Review-Analyzer/nltk_data'
nltk.data.path.append(custom_nltk_data_path)
# Initialize SentimentIntensityAnalyzer


def ensure_nltk_resources():
    try:
        # Ensure the necessary NLTK resources are downloaded
        nltk.download('stopwords', download_dir=custom_nltk_data_path, quiet=True)
        nltk.download('punkt', download_dir=custom_nltk_data_path, quiet=True)
        nltk.download('wordnet', download_dir=custom_nltk_data_path, quiet=True)
        nltk.download('vader_lexicon', download_dir=custom_nltk_data_path, quiet=True)
        print("NLTK resources downloaded successfully.")
    except Exception as e:
        print(f"Error downloading NLTK resources: {e}")

# Ensure the necessary NLTK resources are downloaded
ensure_nltk_resources()
sid = SentimentIntensityAnalyzer()

# Task to ingest data from a CSV file
@task
def ingest_data(directory='./data'):
    """
    Ingest the first CSV file found in the specified directory that includes 'hello' in its name.

    Parameters:
    directory (str): The path to the directory containing the CSV files. Default is './data'.

    Returns:
    DataFrame: The DataFrame containing the data from the first matching file.
    """
    # List all files in the directory
    all_files = os.listdir(directory)
    
    # Iterate over the files to find one that includes 'hello' in its name
    for file_name in all_files:
        if 'hello' in file_name.lower() and file_name.endswith('.csv'):
            file_path = os.path.join(directory, file_name)
            print(f"Reading file: {file_name}")
            return pd.read_csv(file_path)
    
    # If no matching file is found, return None or raise an exception
    print("No file found that includes 'hello' in the name.")
    return None


# Task to remove duplicates and handle missing values
@task
def remove_duplicates_and_handle_missing_values(df):
    df.drop_duplicates(subset=['Content'], inplace=True)
    df.dropna(subset=['Content'], inplace=True)
    return df

# Task to clean and preprocess text

def clean_text(text):
    # Convert text to lowercase
    text = text.lower()
    # Remove special characters, punctuation, and symbols
    text = re.sub(r'[^a-zA-Z\s]', '', text)
    # Remove extra whitespaces
    text = re.sub(r'\s+', ' ', text).strip()
    # Tokenize the text
    tokens = word_tokenize(text)
    # Remove stop words
    stop_words = set(stopwords.words('english'))
    filtered_tokens = [word for word in tokens if word not in stop_words]
    # Lemmatization
    lemmatizer = WordNetLemmatizer()
    lemmatized_tokens = [lemmatizer.lemmatize(word) for word in filtered_tokens]
    # Join tokens back into text
    cleaned_text = ' '.join(lemmatized_tokens)
    return cleaned_text

# Task to preprocess the DataFrame
@task
def preprocess_dataframe(df):
    df['Date'] = pd.to_datetime(df['Date'], format='ISO8601', errors='coerce')
    df['cleaned_content'] = df['Content'].apply(clean_text)
    df['cleaned_header'] = df['Header'].apply(clean_text)
    df['Rating'] = df['Rate'].apply(lambda x: int(x.split()[1]))
    df['Date'] = pd.to_datetime(df['Date'])
    df['date_clean'] = df['Date'].dt.date
    df.set_index('Date', inplace=True)

    df['cleaned_content'] = df['cleaned_content'].astype(str)
    df['cleaned_header'] = df['cleaned_header'].astype(str)
    df['word_count_content'] = df['cleaned_content'].apply(lambda x: len(x.split()))
    df['word_count_header'] = df['cleaned_header'].apply(lambda x: len(x.split()))
    df['review_count'] = df['word_count_content']
    df['title_count'] = df['word_count_header']
    df['date_clean'] = pd.to_datetime(df['date_clean'])  # Ensure it's in datetime format
    df['month'] = df['date_clean'].dt.strftime('%Y-%m')
    df['prediction'] = 1  # Initialize with None or NaN
    df['rate'] = df['prediction'] 
    df['text'] = df['cleaned_content']

    return df

# Task to calculate sentiment scores


def calculate_sentiment_scores(text):

    scores = sid.polarity_scores(text)
    return {
        'compound': scores['compound'],
        'negative': scores['neg'],
        'neutral': scores['neu'],
        'positive': scores['pos']
    }

# Task to apply sentiment analysis to the DataFrame
@task
def apply_sentiment_analysis(df):
    df['sentiment_scores_content'] = df['cleaned_content'].apply(calculate_sentiment_scores)
    df['sentiment_scores_header'] = df['cleaned_header'].apply(calculate_sentiment_scores)

    # Extract individual polarity scores
    df['compound_content'] = df['sentiment_scores_content'].apply(lambda x: x['compound'])
    df['negative_content'] = df['sentiment_scores_content'].apply(lambda x: x['negative'])
    df['neutral_content'] = df['sentiment_scores_content'].apply(lambda x: x['neutral'])
    df['positive_content'] = df['sentiment_scores_content'].apply(lambda x: x['positive'])
    df['compound_header'] = df['sentiment_scores_header'].apply(lambda x: x['compound'])
    df['negative_header'] = df['sentiment_scores_header'].apply(lambda x: x['negative'])
    df['neutral_header'] = df['sentiment_scores_header'].apply(lambda x: x['neutral'])
    df['positive_header'] = df['sentiment_scores_header'].apply(lambda x: x['positive'])
    df = df.drop(columns=['sentiment_scores_content'])
    df = df.drop(columns=['sentiment_scores_header'])
    return df

# Task to generate and save the plot
@task
def categorize_compound_scores(df):
    def categorize_compound(compound):
        if compound <= -0.05:
            return 'negative'
        elif compound >= 0.05:
            return 'positive'
        else:
            return 'neutral'

    df['compound_category'] = df['compound_content'].apply(categorize_compound)
    return df  
import pandas as pd

@task
def split_current_referance_data(df, reference_date, sort_by='month'):
    # Ensure 'date_clean' is in datetime format
    df['date_clean'] = pd.to_datetime(df['date_clean'])
    # Split the data
    reference = df[df['date_clean'] <= reference_date]
    current = df[df['date_clean'] > reference_date]
    # Sort the current dataset
    if sort_by in current.columns:
        current = current.sort_values(by=sort_by)
    else:
        raise ValueError(f"Column '{sort_by}' not found in current data.")
    # Print the shapes of the datasets
    print(f"Current Data Shape: {current.shape}")
    print(f"Reference Data Shape: {reference.shape}")
    print(reference.head())
    return reference, current