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
    Aggregates changes by month, category, and specific bot, counting unique publishers for each.
    """
    # Structure: { "YYYY-MM": {"genai": {"GPTBot": {pub1, pub2}}, "search": {"Googlebot": {pub1}}} }
    monthly_changes = defaultdict(lambda: defaultdict(lambda: defaultdict(set)))

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

                        # Find all genai and search bots in the change
                        genai_bots_in_change = affected_agents.intersection(genai_bots)
                        search_bots_in_change = affected_agents.intersection(search_bots)

                        # Attribute the change to the publisher for each specific bot
                        for bot in genai_bots_in_change:
                            monthly_changes[month_key]['genai'][bot].add(publisher_dir)
                        
                        # Use elif to avoid double-counting if a change affects both
                        if not genai_bots_in_change:
                            for bot in search_bots_in_change:
                                monthly_changes[month_key]['search'][bot].add(publisher_dir)

            except (json.JSONDecodeError, KeyError) as e:
                print(f"Warning: Could not process file {timeline_path}. Error: {e}")

    return monthly_changes

def create_monthly_trend_graph(data, years, output_filename_template="robots_txt_monthly_trends_{years}.png"):
    """
    Creates a stacked bar chart of unique publishers making changes by month
    and prints a detailed breakdown of bots.
    """
    if not data:
        print("No data to plot.")
        return

    # --- Data processing for plotting main categories ---
    # Aggregate publishers across all bots within 'genai' and 'search' for each month
    category_plot_data = defaultdict(lambda: defaultdict(set))
    for month, categories in data.items():
        for category, bots in categories.items():
            for bot, publishers in bots.items():
                category_plot_data[month][category].update(publishers)

    # Convert sets to counts for plotting
    plot_data = {
        month: {category: len(publishers) for category, publishers in values.items()}
        for month, values in category_plot_data.items()
    }

    df = pd.DataFrame.from_dict(plot_data, orient='index').fillna(0)
    df.index = pd.to_datetime(df.index)
    df = df.sort_index()
    
    start_year = min(int(y) for y in years)
    end_year = max(int(y) for y in years)
    all_months = pd.date_range(start=f'{start_year}-01-01', end=f'{end_year}-12-01', freq='MS')
    df = df.reindex(all_months, fill_value=0)
    df.index = df.index.strftime('%Y-%m')

    # --- Plotting ---
    plt.style.use('seaborn-v0_8-whitegrid')
    fig, ax = plt.subplots(figsize=(20, 12)) # Increased figure size

    df.plot(kind='bar', stacked=True, ax=ax, color=['#4c72b0', '#dd8452'], width=0.8)

    title_years = f"{start_year}-{end_year}" if start_year != end_year else str(start_year)
    ax.set_title(f'Unique Publishers Changing robots.txt for Search vs. GenAI Bots ({title_years})', fontsize=20, pad=20)
    ax.set_xlabel('Month', fontsize=14)
    ax.set_ylabel('Number of Publishers Making Changes', fontsize=14)
    ax.tick_params(axis='x', rotation=45, labelsize=12)
    ax.legend(title='Bot Category')

    for container in ax.containers:
        ax.bar_label(container, label_type='center', fmt='%d', fontsize=10, color='white', weight='bold')

    plt.tight_layout(rect=[0, 0.05, 1, 1]) # Adjust layout
    output_filename = output_filename_template.format(years=title_years)
    plt.savefig(output_filename)
    print(f"\nGraph saved to {output_filename}")

    # --- Detailed Console Output ---
    print("\n--- Monthly Breakdown by Bot ---")
    for month in sorted(data.keys()):
        print(f"\n# {month}")
        month_data = data[month]
        for category in ['genai', 'search']:
            if category in month_data:
                print(f"  [{category.upper()}]")
                # Sort bots by the number of publishers they affected
                sorted_bots = sorted(month_data[category].items(), key=lambda item: len(item[1]), reverse=True)
                for bot, publishers in sorted_bots:
                    print(f"    - {bot}: {len(publishers)} publishers")


if __name__ == "__main__":
    publishers_root = "publishers"
    analysis_years = ["2023", "2024", "2025"]
    
    if not os.path.isdir(publishers_root):
        print(f"Error: Directory '{publishers_root}' not found.")
        print("Please run this script from the root of your 'waybackrobots' project directory.")
    else:
        print("Parsing bot categories from bots.txt...")
        search_bots, genai_bots = parse_bots_file()
        
        print(f"Analyzing timeline files for {analysis_years} across all publishers...")
        monthly_data = analyze_timelines_by_month(publishers_root, search_bots, genai_bots, analysis_years)
        
        print("Creating monthly trend graph and detailed breakdown...")
        create_monthly_trend_graph(monthly_data, analysis_years)
