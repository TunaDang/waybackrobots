import json
import os
from datetime import datetime, timedelta
import pandas as pd

def get_company_bots(bots_file):
    """
    Reads the company bots csv file and returns a dictionary of bot_name -> category
    and a list of all bot names.
    """
    df = pd.read_csv(bots_file)
    bots = {}
    
    # AI bots
    ai_bots = df[['name of AI bot']].dropna()
    for bot in ai_bots['name of AI bot']:
        bots[bot] = 'AI'
        
    # Search bots
    search_bots = df[['name of search bot']].dropna()
    for bot in search_bots['name of search bot']:
        bots[bot] = 'Search'
        
    return bots, list(bots.keys())


def get_all_events_for_publisher(publisher_path):
    """
    Scans for timeline_YYYY.json files in year subdirectories and returns a single sorted list of events.
    """
    all_events = []
    if not os.path.isdir(publisher_path):
        return all_events

    for year_dir in os.listdir(publisher_path):
        if not year_dir.isdigit():
            continue
        
        timeline_path = os.path.join(publisher_path, year_dir, f'timeline_{year_dir}.json')
        if os.path.exists(timeline_path):
            try:
                with open(timeline_path, 'r') as f:
                    events = json.load(f)
                if events:
                    all_events.extend(events)
            except (json.JSONDecodeError, IOError) as e:
                print(f"Could not read or parse {timeline_path}: {e}")

    # Sort all events by timestamp
    all_events.sort(key=lambda x: x.get('timestamp', ''))
    return all_events

def get_popular_bots(publishers_dir, bots_to_check):
    """
    Scans the timeline files to identify which of the specified bots are mentioned.
    """
    mentioned_bots = set()
    for publisher in os.listdir(publishers_dir):
        publisher_path = os.path.join(publishers_dir, publisher)
        events = get_all_events_for_publisher(publisher_path)
        
        for event in events:
            if 'initial_content' in event:
                for rule in event['initial_content']:
                    if rule.get('user_agent') in bots_to_check:
                        mentioned_bots.add(rule['user_agent'])
            if 'agents_added' in event:
                for agent in event['agents_added']:
                    if agent in bots_to_check:
                        mentioned_bots.add(agent)
    return list(mentioned_bots)

def get_publishers(publishers_file, num_publishers):
    """
    Reads the list of publishers from a file.
    """
    with open(publishers_file, 'r') as f:
        publishers = [line.strip() for line in f.readlines() if line.strip()]
    return publishers[:num_publishers]

def get_date_range(publishers_dir, publishers):
    min_date = datetime.now()
    max_date = datetime(1970, 1, 1)
    for publisher in publishers:
        publisher_path = os.path.join(publishers_dir, publisher)
        events = get_all_events_for_publisher(publisher_path)
        for event in events:
            timestamp_str = event.get("timestamp")
            if timestamp_str:
                event_date = datetime.strptime(timestamp_str, "%Y%m%d%H%M%S")
                if event_date < min_date:
                    min_date = event_date
                if event_date > max_date:
                    max_date = event_date
    return min_date, max_date if max_date > min_date else datetime.now()


def create_blocking_timeseries(publishers, bots, bot_categories, publishers_dir, output_file, start_date, end_date):
    """
    Creates a CSV file with the blocking status of each bot for each publisher over the last 365 days.
    """
    
    with open(output_file, 'w') as f:
        f.write("date,publisher,bot_name,bot_category,is_blocked\n")

        for publisher in publishers:
            publisher_path = os.path.join(publishers_dir, publisher)
            bot_blocking_history = {bot: {} for bot in bots}

            timeline_data = get_all_events_for_publisher(publisher_path)
            
            if timeline_data:
                for event in timeline_data:
                    timestamp_str = event.get("timestamp")
                    if not timestamp_str:
                        continue
                    
                    event_date = datetime.strptime(timestamp_str, "%Y%m%d%H%M%S")

                    # Process initial content
                    if 'initial_content' in event:
                        for bot in bots:
                            bot_blocked = False
                            for rule in event['initial_content']:
                                if rule.get('user_agent') == bot and rule.get('disallow'):
                                    bot_blocked = True
                                    break
                            bot_blocking_history[bot][event_date] = bot_blocked

                    # Process rule changes
                    if 'rule_changes' in event:
                         for rule_change in event['rule_changes']:
                             agent = rule_change.get('user_agent')
                             if agent in bots:
                                 # Check for broad disallow rules
                                 disallow_rules = rule_change.get('disallow', {})
                                 if isinstance(disallow_rules, dict) and disallow_rules.get('added') == ['https://cnbc.com/']:
                                     bot_blocking_history[agent][event_date] = True
                                 elif rule_change.get('disallow'):
                                     bot_blocking_history[agent][event_date] = True
                                 elif not rule_change.get('disallow') and not rule_change.get('allow'):
                                     bot_blocking_history[agent][event_date] = False


                    # Process agents added
                    if 'agents_added' in event:
                        for agent in event['agents_added']:
                            if agent in bots:
                                # This requires looking at the associated rule_changes to be certain
                                # We will check rule_changes for a corresponding block
                                for rc in timeline_data:
                                    if rc.get('timestamp') == timestamp_str:
                                        for rule_change in rc.get('rule_changes', []):
                                            if rule_change.get('user_agent') == agent:
                                                disallow_rules = rule_change.get('disallow', {})
                                                if isinstance(disallow_rules, dict) and disallow_rules.get('added') == ['https://cnbc.com/']:
                                                    bot_blocking_history[agent][event_date] = True
                                                elif rule_change.get('disallow'):
                                                     bot_blocking_history[agent][event_date] = True


                    # Process agents removed
                    if 'agents_removed' in event:
                        for agent in event['agents_removed']:
                            if agent in bots:
                                bot_blocking_history[agent][event_date] = False


            # Generate daily status
            total_days = (end_date - start_date).days
            for day_delta in range(total_days + 1):
                current_date = start_date + timedelta(days=day_delta)
                date_str = current_date.strftime("%Y-%m-%d")

                for bot in bots:
                    is_blocked = 0
                    
                    # Find the last known state for the bot on or before the current date
                    sorted_dates = sorted([d for d in bot_blocking_history[bot] if d.date() <= current_date.date()], reverse=True)
                    
                    if sorted_dates:
                        last_status_date = sorted_dates[0]
                        is_blocked = 1 if bot_blocking_history[bot][last_status_date] else 0
                    
                    f.write(f"{date_str},{publisher},{bot},{bot_categories.get(bot, 'Unknown')},{is_blocked}\n")
            
            # Print out the final blocking status for the publisher
            print(f"\nBlocking status for {publisher}:")
            blocked_bots_for_publisher = []
            for bot in bots:
                is_blocked = 0
                sorted_dates = sorted([d for d in bot_blocking_history[bot] if d.date() <= end_date.date()], reverse=True)
                if sorted_dates:
                    last_status_date = sorted_dates[0]
                    is_blocked = 1 if bot_blocking_history[bot][last_status_date] else 0
                
                if is_blocked:
                    blocked_bots_for_publisher.append(bot)
            
            if blocked_bots_for_publisher:
                for bot in blocked_bots_for_publisher:
                    print(f"  - Blocks {bot}")
            else:
                print("  - No popular bots blocked.")


def main():
    publishers_dir = '/home/tuan/waybackrobots/publishers'
    publishers_file = '/home/tuan/waybackrobots/ranked_us_publishers.txt'
    bots_file = '/home/tuan/waybackrobots/company_bots.csv'
    output_csv = '/home/tuan/waybackrobots/bot_blocking_analysis.csv'
    
    # Get list of bots to check from company_bots.csv
    bot_categories, bots_to_check = get_company_bots(bots_file)

    print(f"Found {len(bots_to_check)} bots to analyze from {bots_file}")

    # Get publishers
    num_publishers = 100 # Starting with 100 as requested
    publishers = get_publishers(publishers_file, num_publishers)
    print(f"Analyzing the first {num_publishers} publishers: {publishers}")

    start_date, end_date = get_date_range(publishers_dir, publishers)
    print(f"Data ranges from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")

    # Create the time-series CSV
    create_blocking_timeseries(publishers, bots_to_check, bot_categories, publishers_dir, output_csv, start_date, end_date)
    print(f"Analysis complete. The data has been saved to {output_csv}")

if __name__ == '__main__':
    main()