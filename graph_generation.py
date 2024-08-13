import re
import os
import pandas as pd
import seaborn as sns
from wordcloud import WordCloud
from prefect import task
import matplotlib
import matplotlib.pyplot as plt
matplotlib.use('Agg')
from prefect import get_run_logger

@task
def generate_plot(df, folder_path='image'):
    # Ensure the folder exists
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)

    df['date_clean'] = pd.to_datetime(df['date_clean'])

    # Plot for all years together
    plt.figure(figsize=(13, 7))
    years = sorted(df['date_clean'].dt.year.unique())
    for year in years:
        df_year = df[df['date_clean'].dt.year == year].copy()
        monthly_avg_rate = df_year.groupby(df_year['date_clean'].dt.to_period('M'))['Rating'].mean()
        monthly_avg_rate.plot(marker='o', linestyle='-', label=f'Year {year}')

    plt.xlabel('Month')
    plt.ylabel('Average Rate')
    plt.title('Average Monthly Rate for All Years')
    plt.grid(True)
    plt.xticks(rotation=45)
    plt.legend(title='Year')
    plt.tight_layout()

    # Save the combined plot
    all_years_plot_path = os.path.join(folder_path, 'average_monthly_rate_all_years.png')
    plt.savefig(all_years_plot_path)
    plt.close()

    # Log the combined plot path
    logger = get_run_logger()
    logger.info(f"Combined plot saved at {all_years_plot_path}")

    # Generate individual plots for each year
    for year in years:
        df_year = df[df['date_clean'].dt.year == year].copy()
        monthly_avg_rate = df_year.groupby(df_year['date_clean'].dt.to_period('M'))['Rating'].mean()

        plt.figure(figsize=(13, 5))
        monthly_avg_rate.plot(marker='o', linestyle='-')
        plt.xlabel('Month')
        plt.ylabel('Average Rate')
        plt.title(f'Average Monthly Rate for {year}')
        plt.grid(True)
        plt.xticks(rotation=45)
        plt.tight_layout()

        # Save individual plots
        year_plot_path = os.path.join(folder_path, f'average_monthly_rate_{year}.png')
        plt.savefig(year_plot_path)
        plt.close()

        # Log individual plot path
        logger.info(f"Plot for year {year} saved at {year_plot_path}")

    return all_years_plot_path

@task
def generate_wordcloud(df, folder_path='image'):
    # Ensure the folder exists
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)

    # Combine all cleaned content into a single string
    text = ' '.join(df['cleaned_content'])

    # Generate word cloud
    wordcloud = WordCloud(width=1500, height=800, background_color='black').generate(text)

    # Plot and save the word cloud
    plt.figure(figsize=(15, 8))
    plt.imshow(wordcloud, interpolation='bilinear')
    plt.axis('off')

    # Define the path for saving the plot
    plot_path = os.path.join(folder_path, 'wordcloud.png')
    plt.savefig(plot_path, bbox_inches='tight', pad_inches=0, transparent=True)  # Transparent background to avoid any margin color
    plt.close()

    # Log the plot path
    logger = get_run_logger()
    logger.info(f"Word cloud saved at {plot_path}")

    return plot_path
@task
def generate_barplot(df, folder_path='image'):
    # Ensure the folder exists
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)

    # Generate the bar plot
    plt.figure(figsize=(10, 6))
    ax = sns.barplot(data=df, x='Rating', y='compound_content')
    plt.title('Bar Plot of Compound Sentiment Scores vs. Rate')
    plt.xlabel('Cleaned Rate')
    plt.ylabel('Compound Sentiment Score')

    # Save the plot
    plot_path = os.path.join(folder_path, 'sentiment_vs_rate.png')
    plt.savefig(plot_path)
    plt.close()

    # Log the plot path
    logger = get_run_logger()
    logger.info(f"Bar plot saved at {plot_path}")

    return plot_path
# Task to generate and save multiple bar plots
@task
def generate_multiple_barplots(df, folder_path='image'):
    # Ensure the folder exists
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)

    # Create subplots
    fig, axs = plt.subplots(1, 3, figsize=(15, 8))

    # Define a list of sentiment types and corresponding axes
    sentiment_types = ['positive_content', 'neutral_content', 'negative_content']
    titles = ['Positive Sentiment by Rate', 'Neutral Sentiment by Rate', 'Negative Sentiment by Rate']

    for ax, sentiment, title in zip(axs, sentiment_types, titles):
        sns.barplot(data=df, x='Rating', y=sentiment, ax=ax)

        # Set title
        ax.set_title(title)
        ax.set_xlabel('Rate')
        ax.set_ylabel('Sentiment Score')

    plt.tight_layout()

    # Save the plot
    plot_path = os.path.join(folder_path, 'multiple_barplots.png')
    plt.savefig(plot_path)
    plt.close()

    # Log the plot path
    logger = get_run_logger()
    logger.info(f"Multiple bar plots saved at {plot_path}")

    return plot_path
 
