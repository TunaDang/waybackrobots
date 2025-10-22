import os
import json
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

def analyze_timeline_files(root_dir):
    """
    Finds all timeline_2023.json files, counts the changes,
    and returns a dictionary of publisher -> change_count.
    """
    publisher_changes = {}

    for publisher_dir in os.listdir(root_dir):
        publisher_path = os.path.join(root_dir, publisher_dir)
        if os.path.isdir(publisher_path):
            timeline_path = os.path.join(publisher_path, "2023", "timeline_2023.json")
            
            if os.path.exists(timeline_path):
                try:
                    with open(timeline_path, 'r', encoding='utf-8') as f:
                        timeline_data = json.load(f)
                        # Each entry in the list is a recorded change.
                        num_changes = len(timeline_data)
                        publisher_changes[publisher_dir] = num_changes
                except json.JSONDecodeError:
                    print(f"Warning: Could not decode JSON from {timeline_path}")
                except Exception as e:
                    print(f"Error reading file {timeline_path}: {e}")

    return publisher_changes

def create_change_graph(data, output_filename="robots_txt_changes_2023.png"):
    """
    Creates and saves a bar chart of robots.txt changes per publisher.
    """
    if not data:
        print("No data to plot.")
        return

    # Convert to a pandas DataFrame for easier plotting
    df = pd.DataFrame(list(data.items()), columns=['Publisher', 'NumberOfChanges'])
    
    # Sort by number of changes for a cleaner look
    df = df.sort_values('NumberOfChanges', ascending=False)

    # Create the plot
    plt.style.use('seaborn-v0_8-whitegrid')
    plt.figure(figsize=(16, 10))
    
    # Updated barplot call to address the FutureWarning
    ax = sns.barplot(x='Publisher', y='NumberOfChanges', data=df, hue='Publisher', palette='viridis', legend=False)

    ax.set_title('Number of robots.txt Changes in 2023 per Publisher', fontsize=20, pad=20)
    ax.set_xlabel('Publisher', fontsize=14)
    ax.set_ylabel('Number of Detected Changes', fontsize=14)
    
    # Rotate labels to prevent overlap
    plt.xticks(rotation=45, ha='right')
    
    # Add labels on top of bars
    for container in ax.containers:
        ax.bar_label(container, fmt='%d', fontsize=10, padding=3)

    plt.tight_layout()
    
    # Save the figure
    plt.savefig(output_filename)
    print(f"Graph saved to {output_filename}")
    
    # The plot is saved to a file, so we don't need to try to show it in a non-interactive environment.
    # plt.show()


if __name__ == "__main__":
    # The script assumes it's run from the root of the 'waybackrobots' directory
    publishers_root = "publishers"
    
    if not os.path.isdir(publishers_root):
        print(f"Error: Directory '{publishers_root}' not found.")
        print("Please run this script from the root of your 'waybackrobots' project directory.")
    else:
        change_data = analyze_timeline_files(publishers_root)
        create_change_graph(change_data)
