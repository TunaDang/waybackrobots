import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.lines import Line2D

def visualize_blocking_share(csv_file, output_image, company_bots_file):
    """
    Reads the bot blocking analysis data and creates a visualization
    of the share of publishers blocking popular bots over time,
    with separate legends for AI and Search bots.
    """
    df = pd.read_csv(csv_file)
    df['date'] = pd.to_datetime(df['date'])

    # Read company bots to filter
    company_df = pd.read_csv(company_bots_file)
    ai_bots_list = company_df['name of AI bot'].dropna().tolist()
    search_bots_list = company_df['name of search bot'].dropna().tolist()
    all_company_bots = ai_bots_list + search_bots_list
    
    print(f"AI bots from company_bots.csv: {ai_bots_list}")
    print(f"Search bots from company_bots.csv: {search_bots_list}")

    # Filter to only include bots from company_bots.csv
    df_filtered = df[df['bot_name'].isin(all_company_bots)]

    # Calculate the share of publishers blocking each bot for each day
    blocking_share = df_filtered.groupby(['date', 'bot_name', 'bot_category'])['is_blocked'].mean().reset_index()
    blocking_share = blocking_share.rename(columns={'is_blocked': 'share_blocking'})

    # Convert share to percentage
    blocking_share['share_blocking'] *= 100

    # Separate bots by category
    ai_bots = blocking_share[blocking_share['bot_category'] == 'AI']
    search_bots = blocking_share[blocking_share['bot_category'] == 'Search']

    plt.style.use('seaborn-v0_8-whitegrid')
    fig, ax = plt.subplots(figsize=(15, 10))

    # Define color palettes
    ai_colors = plt.cm.Reds(range(50, 256, 256 // (len(ai_bots['bot_name'].unique()) + 1)))
    search_colors = plt.cm.Blues(range(50, 256, 256 // (len(search_bots['bot_name'].unique()) + 1)))

    # Plot AI bots with solid lines
    if not ai_bots.empty:
        ai_pivot = ai_bots.pivot(index='date', columns='bot_name', values='share_blocking')
        for i, col in enumerate(ai_pivot.columns):
            ax.plot(ai_pivot.index, ai_pivot[col], marker='o', linestyle='-', 
                   markersize=4, label=col, color=ai_colors[i])

    # Plot Search bots with dashed lines
    if not search_bots.empty:
        search_pivot = search_bots.pivot(index='date', columns='bot_name', values='share_blocking')
        for i, col in enumerate(search_pivot.columns):
            ax.plot(search_pivot.index, search_pivot[col], marker='^', linestyle='--', 
                   markersize=4, label=col, color=search_colors[i])

    ax.set_title('Share of Top 100 Publishers Blocking Company Bots Over Time', fontsize=16)
    ax.set_xlabel('Date', fontsize=12)
    ax.set_ylabel('Share of Publishers Blocking Bot (%)', fontsize=12)
    ax.axhline(y=0, color='gray', linestyle='--', linewidth=0.8)
    ax.grid(True)

    # Create custom legends in top left
    handles, labels = ax.get_legend_handles_labels()
    
    ai_bot_names = ai_bots['bot_name'].unique()
    search_bot_names = search_bots['bot_name'].unique()
    
    ai_legend_handles = [h for h, l in zip(handles, labels) if l in ai_bot_names]
    ai_legend_labels = [l for l in labels if l in ai_bot_names]
    
    search_legend_handles = [h for h, l in zip(handles, labels) if l in search_bot_names]
    search_legend_labels = [l for l in labels if l in search_bot_names]

    # Position legends in top left corner
    if ai_legend_handles:
        ai_legend = ax.legend(ai_legend_handles, ai_legend_labels, 
                            title='AI Bots', 
                            loc='upper left', 
                            bbox_to_anchor=(0.01, 0.99),
                            framealpha=0.9)
        ax.add_artist(ai_legend)

    if search_legend_handles:
        # Calculate vertical position based on number of AI bots
        vertical_offset = 0.99 - (len(ai_legend_labels) + 2) * 0.05
        search_legend = ax.legend(search_legend_handles, search_legend_labels, 
                                 title='Search Bots', 
                                 loc='upper left', 
                                 bbox_to_anchor=(0.01, vertical_offset),
                                 framealpha=0.9)
        ax.add_artist(search_legend)
    
    plt.tight_layout()
    plt.savefig(output_image, dpi=300, bbox_inches='tight')
    print(f"Visualization saved to {output_image}")

def main():
    csv_file = '/home/tuan/waybackrobots/bot_blocking_analysis.csv'
    company_bots_file = '/home/tuan/waybackrobots/company_bots.csv'
    output_image = '/home/tuan/waybackrobots/bot_blocking_share.png'
    visualize_blocking_share(csv_file, output_image, company_bots_file)

if __name__ == '__main__':
    main()
