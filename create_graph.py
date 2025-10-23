import os
import json
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
from collections import defaultdict
from datetime import datetime

def parse_bots_file(filepath="bots.txt"):
    """
    Parses the bots.txt file to categorize bots into 'search' and 'genai'.
    Returns two sets of bot names for fast lookups.
    """
    search_bots, genai_bots = set(), set()
    current_category = None
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if "Search Engine Crawlers" in line:
                    current_category = 'search'
                elif "Generative AI Crawlers" in line:
                    current_category = 'genai'
                elif line.startswith("User-agent:"):
                    bot_name = line.replace("User-agent:", "").strip()
                    if current_category == 'search':
                        search_bots.add(bot_name)
                    elif current_category == 'genai':
                        genai_bots.add(bot_name)
    except FileNotFoundError:
        print(f"Warning: '{filepath}' not found. Cannot categorize bots.")
    return search_bots, genai_bots

def analyze_timelines_by_month(root_dir, search_bots, genai_bots, years):
    """
    Aggregates the number of unique publishers making changes by month and bot category for given years.
    """
    # Structure: { "YYYY-MM": {"search": {publisher1, publisher2}, "genai": {publisher3}} }
    monthly_changes = defaultdict(lambda: defaultdict(set))

    for publisher_dir in os.listdir(root_dir):
        publisher_path = os.path.join(root_dir, publisher_dir)
        if not os.path.isdir(publisher_path):
            continue

        for year in years:
            timeline_path = os.path.join(publisher_path, year, f"timeline_{year}.json")
            if not os.path.exists(timeline_path):
                continue

            try:
                with open(timeline_path, 'r', encoding='utf-8') as f:
                    timeline_data = json.load(f)
                    for change in timeline_data:
                        if "initial_content" in change:
                            continue

                        ts = change.get("timestamp")
                        if not ts or len(ts) < 6:
                            continue
                        month_key = f"{ts[:4]}-{ts[4:6]}"

                        affected_agents = set(change.get("agents_added", []))
                        for rule_change in change.get("rule_changes", []):
                            affected_agents.add(rule_change.get("user_agent"))

                        is_genai_change = any(agent in genai_bots for agent in affected_agents)
                        is_search_change = any(agent in search_bots for agent in affected_agents)

                        if is_genai_change:
                            monthly_changes[month_key]['genai'].add(publisher_dir)
                        elif is_search_change:
                            monthly_changes[month_key]['search'].add(publisher_dir)

            except (json.JSONDecodeError, KeyError) as e:
                print(f"Warning: Could not process file {timeline_path}. Error: {e}")

    return monthly_changes

def create_monthly_trend_graph(data, years, output_filename_template="robots_txt_monthly_trends_{years}.png"):
    """
    Creates a stacked bar chart of unique publishers making changes by month.
    """
    if not data:
        print("No data to plot.")
        return

    # Convert the sets of publishers to counts for plotting
    plot_data = {
        month: {category: len(publishers) for category, publishers in values.items()}
        for month, values in data.items()
    }

    df = pd.DataFrame.from_dict(plot_data, orient='index').fillna(0)
    df.index = pd.to_datetime(df.index)
    df = df.sort_index()
    
    start_year = min(years)
    end_year = max(years)
    all_months = pd.date_range(start=f'{start_year}-01-01', end=f'{end_year}-12-01', freq='MS')
    df = df.reindex(all_months, fill_value=0)
    df.index = df.index.strftime('%Y-%m')

    plt.style.use('seaborn-v0_8-whitegrid')
    fig, ax = plt.subplots(figsize=(16, 10))

    df.plot(kind='bar', stacked=True, ax=ax, color=['#4c72b0', '#dd8452'], width=0.8)

    title_years = f"{start_year}-{end_year}" if start_year != end_year else start_year
    ax.set_title(f'Unique Publishers Changing robots.txt for Search vs. GenAI Bots ({title_years})', fontsize=20, pad=20)
    ax.set_xlabel('Month', fontsize=14)
    ax.set_ylabel('Number of Publishers Making Changes', fontsize=14)
    ax.tick_params(axis='x', rotation=45)
    ax.legend(title='Bot Category')

    for container in ax.containers:
        ax.bar_label(container, label_type='center', fmt='%d', fontsize=10, color='white', weight='bold')

    plt.tight_layout()
    output_filename = output_filename_template.format(years=title_years)
    plt.savefig(output_filename)
    print(f"Graph saved to {output_filename}")


if __name__ == "__main__":
    publishers_root = "publishers"
    analysis_years = ["2023", "2024"]
    
    if not os.path.isdir(publishers_root):
        print(f"Error: Directory '{publishers_root}' not found.")
        print("Please run this script from the root of your 'waybackrobots' project directory.")
    else:
        print("Parsing bot categories from bots.txt...")
        search_bots, genai_bots = parse_bots_file()
        
        print(f"Analyzing timeline files for {analysis_years} across all publishers...")
        monthly_data = analyze_timelines_by_month(publishers_root, search_bots, genai_bots, analysis_years)
        
        print("Creating monthly trend graph...")
        create_monthly_trend_graph(monthly_data, analysis_years)
