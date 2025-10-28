import pandas as pd
import numpy as np
import statsmodels.api as sm
import statsmodels.formula.api as smf
from datetime import datetime
import matplotlib.pyplot as plt
import seaborn as sns

def load_and_prepare_data(csv_file, publishers_file, company_bots_file):
    """
    Load the blocking data and prepare it for regression analysis.
    """
    # Load blocking data
    df = pd.read_csv(csv_file)
    df['date'] = pd.to_datetime(df['date'])
    
    # Load publisher rankings
    with open(publishers_file, 'r') as f:
        publishers = [line.strip() for line in f.readlines() if line.strip()]
    
    # Create rank mapping (1-indexed: nytimes.com = 1, cnn.com = 2, etc.)
    publisher_rank = {pub: idx + 1 for idx, pub in enumerate(publishers)}
    df['rank'] = df['publisher'].map(publisher_rank)
    
    # Filter to only publishers with ranks
    df = df[df['rank'].notna()].copy()
    
    # Create log(rank) variable
    df['log_rank'] = np.log(df['rank'])
    
    # Create AI bot indicator (1 if AI bot, 0 if search bot)
    df['ai_bot'] = (df['bot_category'] == 'AI').astype(int)
    
    # Create interaction term
    df['ai_bot_x_log_rank'] = df['ai_bot'] * df['log_rank']
    
    # Create time fixed effects (year-month)
    df['year_month'] = df['date'].dt.to_period('M').astype(str)
    
    # Create time trend (days since first observation)
    df['days_since_start'] = (df['date'] - df['date'].min()).dt.days
    
    print(f"Dataset shape: {df.shape}")
    print(f"Date range: {df['date'].min()} to {df['date'].max()}")
    print(f"Number of publishers: {df['publisher'].nunique()}")
    print(f"Number of bots: {df['bot_name'].nunique()}")
    print(f"AI bots: {df[df['ai_bot']==1]['bot_name'].unique()}")
    print(f"Search bots: {df[df['ai_bot']==0]['bot_name'].unique()}")
    
    return df

def run_basic_regression(df):
    """
    Run the main regression with time fixed effects.
    """
    print("\n" + "="*80)
    print("MODEL 1: Basic Model with Time Fixed Effects")
    print("="*80)
    
    # Model with time fixed effects
    formula = 'is_blocked ~ ai_bot + log_rank + ai_bot_x_log_rank + C(year_month)'
    model = smf.ols(formula, data=df)
    results = model.fit(cov_type='cluster', cov_kwds={'groups': df['publisher']})
    
    print(results.summary())
    
    # Extract key coefficients
    print("\n" + "-"*80)
    print("KEY COEFFICIENTS INTERPRETATION:")
    print("-"*80)
    
    try:
        ai_bot_coef = results.params['ai_bot']
        log_rank_coef = results.params['log_rank']
        interaction_coef = results.params['ai_bot_x_log_rank']
        
        print(f"\nAI Bot (β₁): {ai_bot_coef:.4f} (p={results.pvalues['ai_bot']:.4f})")
        print(f"  → AI bots are blocked {ai_bot_coef*100:.2f} percentage points {'more' if ai_bot_coef > 0 else 'less'} than search bots (for rank=1)")
        
        print(f"\nLog(Rank) (β₂): {log_rank_coef:.4f} (p={results.pvalues['log_rank']:.4f})")
        print(f"  → A 1% increase in rank is associated with {log_rank_coef*100:.2f} percentage point change in blocking search bots")
        
        print(f"\nAI Bot × Log(Rank) (β₃): {interaction_coef:.4f} (p={results.pvalues['ai_bot_x_log_rank']:.4f})")
        if results.pvalues['ai_bot_x_log_rank'] < 0.05:
            if interaction_coef > 0:
                print(f"  → **SIGNIFICANT**: Smaller publishers (higher rank) are MORE likely to block AI bots relative to search bots")
                print(f"  → For each unit increase in log(rank), AI bots are blocked {interaction_coef*100:.2f} percentage points more than search bots")
            else:
                print(f"  → **SIGNIFICANT**: Larger publishers (lower rank) are MORE likely to block AI bots relative to search bots")
                print(f"  → For each unit increase in log(rank), AI bots are blocked {abs(interaction_coef)*100:.2f} percentage points less than search bots")
        else:
            print(f"  → NOT SIGNIFICANT: No differential effect of publisher size on AI vs search bot blocking")
        
        # Calculate marginal effects at different ranks
        print("\n" + "-"*80)
        print("MARGINAL EFFECTS: Difference in blocking rates (AI - Search) at different ranks:")
        print("-"*80)
        for rank in [1, 5, 10, 25, 50, 100]:
            marginal_effect = ai_bot_coef + interaction_coef * np.log(rank)
            print(f"Rank {rank:3d} (e.g., {df[df['rank']==rank]['publisher'].iloc[0] if rank <= len(df['publisher'].unique()) else 'N/A'}): "
                  f"{marginal_effect*100:+.2f} percentage points")
    
    except KeyError as e:
        print(f"Coefficient not found: {e}")
    
    return results

def run_alternative_models(df):
    """
    Run alternative model specifications for robustness.
    """
    print("\n" + "="*80)
    print("MODEL 2: With Bot Fixed Effects")
    print("="*80)
    
    # Model with bot fixed effects instead of just AI indicator
    formula = 'is_blocked ~ C(bot_name) + log_rank + C(bot_name):log_rank + C(year_month)'
    model = smf.ols(formula, data=df)
    results2 = model.fit(cov_type='cluster', cov_kwds={'groups': df['publisher']})
    print(results2.summary())
    
    print("\n" + "="*80)
    print("MODEL 3: With Linear Time Trend (Instead of Time FE)")
    print("="*80)
    
    formula = 'is_blocked ~ ai_bot + log_rank + ai_bot_x_log_rank + days_since_start'
    model = smf.ols(formula, data=df)
    results3 = model.fit(cov_type='cluster', cov_kwds={'groups': df['publisher']})
    print(results3.summary())
    
    return results2, results3

def visualize_heterogeneous_effects(df, results):
    """
    Visualize the heterogeneous effects across publisher ranks.
    """
    fig, axes = plt.subplots(1, 2, figsize=(15, 6))
    
    # Extract coefficients
    try:
        ai_bot_coef = results.params['ai_bot']
        interaction_coef = results.params['ai_bot_x_log_rank']
        
        # Plot 1: Predicted marginal effect across ranks
        ranks = np.arange(1, 101)
        log_ranks = np.log(ranks)
        marginal_effects = ai_bot_coef + interaction_coef * log_ranks
        
        axes[0].plot(ranks, marginal_effects * 100, linewidth=2)
        axes[0].axhline(y=0, color='gray', linestyle='--', linewidth=1)
        axes[0].set_xlabel('Publisher Rank (1 = Largest)', fontsize=12)
        axes[0].set_ylabel('Differential Blocking Effect (AI - Search) in %', fontsize=12)
        axes[0].set_title('Heterogeneous Effect of Publisher Size on AI vs Search Bot Blocking', fontsize=13)
        axes[0].grid(True, alpha=0.3)
        
        # Plot 2: Actual blocking rates by rank decile
        df['rank_decile'] = pd.qcut(df['rank'], q=10, labels=False, duplicates='drop') + 1
        
        blocking_by_decile = df.groupby(['rank_decile', 'ai_bot'])['is_blocked'].mean().reset_index()
        
        ai_data = blocking_by_decile[blocking_by_decile['ai_bot'] == 1]
        search_data = blocking_by_decile[blocking_by_decile['ai_bot'] == 0]
        
        axes[1].plot(ai_data['rank_decile'], ai_data['is_blocked'] * 100, 
                    marker='o', label='AI Bots', linewidth=2, markersize=8)
        axes[1].plot(search_data['rank_decile'], search_data['is_blocked'] * 100, 
                    marker='^', label='Search Bots', linewidth=2, markersize=8)
        axes[1].set_xlabel('Publisher Rank Decile (1 = Largest)', fontsize=12)
        axes[1].set_ylabel('Blocking Rate (%)', fontsize=12)
        axes[1].set_title('Actual Blocking Rates by Publisher Size', fontsize=13)
        axes[1].legend()
        axes[1].grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.savefig('/home/tuan/waybackrobots/heterogeneous_effects.png', dpi=300, bbox_inches='tight')
        print("\nVisualization saved to /home/tuan/waybackrobots/heterogeneous_effects.png")
        
    except KeyError as e:
        print(f"Could not create visualization: {e}")

def create_summary_table(df):
    """
    Create descriptive statistics table.
    """
    print("\n" + "="*80)
    print("DESCRIPTIVE STATISTICS")
    print("="*80)
    
    summary = df.groupby('ai_bot').agg({
        'is_blocked': ['mean', 'std', 'count'],
        'rank': 'mean',
        'bot_name': 'nunique'
    }).round(4)
    
    summary.index = ['Search Bots', 'AI Bots']
    print(summary)
    
    # Blocking rate by publisher size
    print("\n" + "-"*80)
    print("BLOCKING RATES BY PUBLISHER SIZE QUARTILE:")
    print("-"*80)
    
    df['rank_quartile'] = pd.qcut(df['rank'], q=4, labels=['Top 25%', 'Q2', 'Q3', 'Bottom 25%'])
    quartile_summary = df.groupby(['rank_quartile', 'ai_bot'])['is_blocked'].mean().unstack()
    quartile_summary.columns = ['Search Bots', 'AI Bots']
    quartile_summary['Difference (AI - Search)'] = quartile_summary['AI Bots'] - quartile_summary['Search Bots']
    print(quartile_summary.round(4))

def main():
    csv_file = '/home/tuan/waybackrobots/bot_blocking_analysis.csv'
    publishers_file = '/home/tuan/waybackrobots/ranked_us_publishers.txt'
    company_bots_file = '/home/tuan/waybackrobots/company_bots.csv'
    
    # Load and prepare data
    df = load_and_prepare_data(csv_file, publishers_file, company_bots_file)
    
    # Descriptive statistics
    create_summary_table(df)
    
    # Main regression
    results = run_basic_regression(df)
    
    # Alternative models for robustness
    results2, results3 = run_alternative_models(df)
    
    # Visualize effects
    visualize_heterogeneous_effects(df, results)
    
    print("\n" + "="*80)
    print("ANALYSIS COMPLETE")
    print("="*80)

if __name__ == '__main__':
    main()