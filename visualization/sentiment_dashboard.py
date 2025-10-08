"""
Sentiment Analysis Dashboard for Lakers Project
Creates visualizations for sentiment vs performance analysis
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SentimentDashboard:
    """
    Dashboard for visualizing Lakers sentiment analysis and performance correlation
    """
    
    def __init__(self):
        self.color_palette = {
            'positive': '#2E8B57',  # Sea Green
            'negative': '#DC143C',  # Crimson
            'neutral': '#4682B4',   # Steel Blue
            'lakers_purple': '#552583',
            'lakers_gold': '#FDB927'
        }
        
        # Set style
        plt.style.use('seaborn-v0_8')
        sns.set_palette("husl")
    
    def create_sentiment_trend_chart(self, sentiment_data: pd.DataFrame, 
                                   player_name: str) -> go.Figure:
        """
        Create sentiment trend chart for a specific player
        
        Args:
            sentiment_data: DataFrame with sentiment data
            player_name: Name of the player
            
        Returns:
            Plotly figure object
        """
        try:
            fig = go.Figure()
            
            # Add sentiment trend line
            fig.add_trace(go.Scatter(
                x=sentiment_data['date'],
                y=sentiment_data['avg_vader_compound'],
                mode='lines+markers',
                name='Sentiment Score',
                line=dict(color=self.color_palette['lakers_purple'], width=3),
                marker=dict(size=8)
            ))
            
            # Add zero line
            fig.add_hline(
                y=0, 
                line_dash="dash", 
                line_color="gray",
                annotation_text="Neutral Sentiment"
            )
            
            # Update layout
            fig.update_layout(
                title=f'{player_name.replace("_", " ").title()} - Sentiment Trend',
                xaxis_title='Date',
                yaxis_title='Sentiment Score (VADER Compound)',
                template='plotly_white',
                height=500,
                showlegend=True
            )
            
            return fig
            
        except Exception as e:
            logger.error(f"Failed to create sentiment trend chart: {e}")
            return go.Figure()
    
    def create_performance_trend_chart(self, performance_data: pd.DataFrame, 
                                     player_name: str) -> go.Figure:
        """
        Create performance trend chart for a specific player
        
        Args:
            performance_data: DataFrame with performance data
            player_name: Name of the player
            
        Returns:
            Plotly figure object
        """
        try:
            fig = make_subplots(
                rows=2, cols=2,
                subplot_titles=('Points', 'Rebounds', 'Assists', 'Plus/Minus'),
                specs=[[{"secondary_y": False}, {"secondary_y": False}],
                       [{"secondary_y": False}, {"secondary_y": False}]]
            )
            
            # Points
            fig.add_trace(
                go.Scatter(
                    x=performance_data['game_date'],
                    y=performance_data['points'],
                    mode='lines+markers',
                    name='Points',
                    line=dict(color=self.color_palette['lakers_gold'])
                ),
                row=1, col=1
            )
            
            # Rebounds
            fig.add_trace(
                go.Scatter(
                    x=performance_data['game_date'],
                    y=performance_data['rebounds'],
                    mode='lines+markers',
                    name='Rebounds',
                    line=dict(color=self.color_palette['lakers_purple'])
                ),
                row=1, col=2
            )
            
            # Assists
            fig.add_trace(
                go.Scatter(
                    x=performance_data['game_date'],
                    y=performance_data['assists'],
                    mode='lines+markers',
                    name='Assists',
                    line=dict(color=self.color_palette['positive'])
                ),
                row=2, col=1
            )
            
            # Plus/Minus
            fig.add_trace(
                go.Scatter(
                    x=performance_data['game_date'],
                    y=performance_data['plus_minus'],
                    mode='lines+markers',
                    name='Plus/Minus',
                    line=dict(color=self.color_palette['negative'])
                ),
                row=2, col=2
            )
            
            fig.update_layout(
                title=f'{player_name.replace("_", " ").title()} - Performance Trends',
                template='plotly_white',
                height=600,
                showlegend=False
            )
            
            return fig
            
        except Exception as e:
            logger.error(f"Failed to create performance trend chart: {e}")
            return go.Figure()
    
    def create_sentiment_vs_performance_scatter(self, combined_data: pd.DataFrame, 
                                              player_name: str) -> go.Figure:
        """
        Create scatter plot showing sentiment vs performance correlation
        
        Args:
            combined_data: DataFrame with both sentiment and performance data
            player_name: Name of the player
            
        Returns:
            Plotly figure object
        """
        try:
            fig = go.Figure()
            
            # Create scatter plot
            fig.add_trace(go.Scatter(
                x=combined_data['avg_vader_compound'],
                y=combined_data['points'],
                mode='markers',
                marker=dict(
                    size=combined_data['mention_count'] * 2,
                    color=combined_data['avg_vader_compound'],
                    colorscale='RdYlGn',
                    showscale=True,
                    colorbar=dict(title="Sentiment Score")
                ),
                text=combined_data['date'],
                hovertemplate='<b>Date:</b> %{text}<br>' +
                             '<b>Sentiment:</b> %{x:.3f}<br>' +
                             '<b>Points:</b> %{y}<br>' +
                             '<b>Mentions:</b> %{marker.size}<br>' +
                             '<extra></extra>',
                name='Games'
            ))
            
            # Add trend line
            if len(combined_data) > 1:
                z = np.polyfit(combined_data['avg_vader_compound'], 
                              combined_data['points'], 1)
                p = np.poly1d(z)
                fig.add_trace(go.Scatter(
                    x=combined_data['avg_vader_compound'],
                    y=p(combined_data['avg_vader_compound']),
                    mode='lines',
                    name='Trend Line',
                    line=dict(color='red', dash='dash')
                ))
            
            fig.update_layout(
                title=f'{player_name.replace("_", " ").title()} - Sentiment vs Performance',
                xaxis_title='Sentiment Score (VADER Compound)',
                yaxis_title='Points Scored',
                template='plotly_white',
                height=500
            )
            
            return fig
            
        except Exception as e:
            logger.error(f"Failed to create sentiment vs performance scatter: {e}")
            return go.Figure()
    
    def create_player_comparison_chart(self, all_players_data: pd.DataFrame) -> go.Figure:
        """
        Create comparison chart for all players
        
        Args:
            all_players_data: DataFrame with all players' sentiment data
            
        Returns:
            Plotly figure object
        """
        try:
            # Sort by total mentions
            all_players_data = all_players_data.sort_values('total_mentions', ascending=True)
            
            fig = go.Figure()
            
            # Create horizontal bar chart
            fig.add_trace(go.Bar(
                y=all_players_data['player_name'].str.replace('_', ' ').str.title(),
                x=all_players_data['total_mentions'],
                orientation='h',
                marker=dict(
                    color=all_players_data['avg_vader_compound'],
                    colorscale='RdYlGn',
                    showscale=True,
                    colorbar=dict(title="Avg Sentiment")
                ),
                text=all_players_data['total_mentions'],
                textposition='auto',
                hovertemplate='<b>%{y}</b><br>' +
                             'Total Mentions: %{x}<br>' +
                             'Avg Sentiment: %{marker.color:.3f}<br>' +
                             '<extra></extra>'
            ))
            
            fig.update_layout(
                title='Lakers Players - Sentiment Analysis Summary',
                xaxis_title='Total Mentions',
                yaxis_title='Player',
                template='plotly_white',
                height=600
            )
            
            return fig
            
        except Exception as e:
            logger.error(f"Failed to create player comparison chart: {e}")
            return go.Figure()
    
    def create_sentiment_distribution_chart(self, sentiment_data: pd.DataFrame) -> go.Figure:
        """
        Create sentiment distribution chart
        
        Args:
            sentiment_data: DataFrame with sentiment data
            
        Returns:
            Plotly figure object
        """
        try:
            fig = go.Figure()
            
            # Create histogram
            fig.add_trace(go.Histogram(
                x=sentiment_data['vader_compound'],
                nbinsx=20,
                marker_color=self.color_palette['lakers_purple'],
                opacity=0.7
            ))
            
            # Add mean line
            mean_sentiment = sentiment_data['vader_compound'].mean()
            fig.add_vline(
                x=mean_sentiment,
                line_dash="dash",
                line_color="red",
                annotation_text=f"Mean: {mean_sentiment:.3f}"
            )
            
            fig.update_layout(
                title='Sentiment Score Distribution',
                xaxis_title='Sentiment Score (VADER Compound)',
                yaxis_title='Frequency',
                template='plotly_white',
                height=400
            )
            
            return fig
            
        except Exception as e:
            logger.error(f"Failed to create sentiment distribution chart: {e}")
            return go.Figure()
    
    def create_correlation_heatmap(self, correlation_data: Dict[str, Any]) -> go.Figure:
        """
        Create correlation heatmap for all players
        
        Args:
            correlation_data: Dictionary with correlation data for all players
            
        Returns:
            Plotly figure object
        """
        try:
            # Prepare data for heatmap
            players = []
            correlation_types = []
            correlation_values = []
            
            for player, data in correlation_data.items():
                correlations = data.get('correlations', {})
                for corr_type, corr_data in correlations.items():
                    if isinstance(corr_data, dict) and 'correlation_coefficient' in corr_data:
                        players.append(player.replace('_', ' ').title())
                        correlation_types.append(corr_type.replace('_', ' ').title())
                        correlation_values.append(corr_data['correlation_coefficient'])
            
            if not correlation_values:
                return go.Figure()
            
            # Create DataFrame
            heatmap_df = pd.DataFrame({
                'Player': players,
                'Correlation Type': correlation_types,
                'Correlation': correlation_values
            })
            
            # Pivot for heatmap
            pivot_df = heatmap_df.pivot(index='Player', columns='Correlation Type', values='Correlation')
            
            # Create heatmap
            fig = go.Figure(data=go.Heatmap(
                z=pivot_df.values,
                x=pivot_df.columns,
                y=pivot_df.index,
                colorscale='RdYlGn',
                zmid=0,
                text=np.round(pivot_df.values, 3),
                texttemplate="%{text}",
                textfont={"size": 10},
                hovertemplate='<b>%{y}</b><br>' +
                             '<b>%{x}</b><br>' +
                             'Correlation: %{z:.3f}<br>' +
                             '<extra></extra>'
            ))
            
            fig.update_layout(
                title='Sentiment-Performance Correlation Heatmap',
                xaxis_title='Correlation Type',
                yaxis_title='Player',
                template='plotly_white',
                height=500
            )
            
            return fig
            
        except Exception as e:
            logger.error(f"Failed to create correlation heatmap: {e}")
            return go.Figure()
    
    def create_dashboard_summary(self, insights: Dict[str, Any]) -> go.Figure:
        """
        Create dashboard summary with key insights
        
        Args:
            insights: Dictionary with analysis insights
            
        Returns:
            Plotly figure object
        """
        try:
            fig = go.Figure()
            
            # Create summary text
            summary_text = f"""
            <b>Lakers Sentiment Analysis Summary</b><br><br>
            
            <b>Analysis Period:</b> {insights.get('analysis_period_days', 'N/A')} days<br>
            <b>Total Players Analyzed:</b> {insights.get('total_players_analyzed', 'N/A')}<br><br>
            
            <b>Key Findings:</b><br>
            """
            
            for finding in insights.get('key_findings', []):
                summary_text += f"• {finding}<br>"
            
            # Add correlation summary
            corr_summary = insights.get('correlation_summary', {})
            if corr_summary:
                summary_text += f"<br><b>Correlation Summary:</b><br>"
                summary_text += f"• Total Correlations: {corr_summary.get('total_correlations', 'N/A')}<br>"
                summary_text += f"• Average Correlation: {corr_summary.get('average_correlation', 0):.3f}<br>"
                summary_text += f"• Strong Correlations: {corr_summary.get('strong_correlations', 'N/A')}<br>"
                summary_text += f"• Moderate Correlations: {corr_summary.get('moderate_correlations', 'N/A')}<br>"
                summary_text += f"• Weak Correlations: {corr_summary.get('weak_correlations', 'N/A')}<br>"
            
            # Add text annotation
            fig.add_annotation(
                text=summary_text,
                xref="paper", yref="paper",
                x=0.5, y=0.5,
                showarrow=False,
                font=dict(size=14),
                align="left",
                bgcolor="rgba(255,255,255,0.8)",
                bordercolor="black",
                borderwidth=1
            )
            
            fig.update_layout(
                title='Analysis Summary',
                template='plotly_white',
                height=400,
                xaxis=dict(visible=False),
                yaxis=dict(visible=False)
            )
            
            return fig
            
        except Exception as e:
            logger.error(f"Failed to create dashboard summary: {e}")
            return go.Figure()
    
    def save_charts(self, figures: Dict[str, go.Figure], output_dir: str = "charts"):
        """
        Save all charts to HTML files
        
        Args:
            figures: Dictionary of figure names and Plotly figures
            output_dir: Output directory for charts
        """
        try:
            import os
            os.makedirs(output_dir, exist_ok=True)
            
            for name, fig in figures.items():
                if fig.data:  # Only save if figure has data
                    filepath = os.path.join(output_dir, f"{name}.html")
                    fig.write_html(filepath)
                    logger.info(f"Saved chart: {filepath}")
            
        except Exception as e:
            logger.error(f"Failed to save charts: {e}")


def test_dashboard():
    """Test function for the dashboard"""
    try:
        print("Testing Sentiment Dashboard:")
        print("=" * 50)
        
        dashboard = SentimentDashboard()
        
        # Create sample data
        sample_sentiment_data = pd.DataFrame({
            'date': pd.date_range('2024-01-01', periods=10, freq='D'),
            'avg_vader_compound': np.random.normal(0, 0.5, 10),
            'mention_count': np.random.randint(1, 20, 10)
        })
        
        sample_performance_data = pd.DataFrame({
            'game_date': pd.date_range('2024-01-01', periods=10, freq='D'),
            'points': np.random.randint(10, 30, 10),
            'rebounds': np.random.randint(5, 15, 10),
            'assists': np.random.randint(3, 10, 10),
            'plus_minus': np.random.randint(-10, 15, 10)
        })
        
        # Test chart creation
        print("Testing sentiment trend chart...")
        sentiment_chart = dashboard.create_sentiment_trend_chart(sample_sentiment_data, 'lebron')
        print(f"Sentiment trend chart: {'PASSED' if sentiment_chart.data else 'FAILED'}")
        
        print("Testing performance trend chart...")
        performance_chart = dashboard.create_performance_trend_chart(sample_performance_data, 'lebron')
        print(f"Performance trend chart: {'PASSED' if performance_chart.data else 'FAILED'}")
        
        print("Testing sentiment distribution chart...")
        distribution_chart = dashboard.create_sentiment_distribution_chart(sample_sentiment_data)
        print(f"Sentiment distribution chart: {'PASSED' if distribution_chart.data else 'FAILED'}")
        
        print("Dashboard test completed successfully!")
        
    except Exception as e:
        print(f"Dashboard test failed: {e}")


if __name__ == "__main__":
    test_dashboard()
