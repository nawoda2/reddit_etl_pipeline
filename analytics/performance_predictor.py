"""
Performance Predictor for Lakers Sentiment Analysis
Builds machine learning models to predict player performance from sentiment data
"""

import pandas as pd
import numpy as np
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from sklearn.linear_model import LinearRegression, Ridge
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.metrics import mean_squared_error, r2_score, mean_absolute_error
from sklearn.preprocessing import StandardScaler
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from analytics.correlation_engine import SentimentPerformanceCorrelationEngine

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PerformancePredictor:
    """
    Machine learning models to predict player performance from sentiment data
    """
    
    def __init__(self):
        self.correlation_engine = SentimentPerformanceCorrelationEngine()
        self.models = {}
        self.scalers = {}
        
        logger.info("PerformancePredictor initialized")
    
    def build_prediction_models(self, player_name: str, 
                              start_date: str = "2023-10-01",
                              end_date: str = "2024-05-31") -> Dict[str, Any]:
        """
        Build prediction models for a specific player
        
        Args:
            player_name: Name of the player
            start_date: Start date for training data
            end_date: End date for training data
            
        Returns:
            Dictionary with model results
        """
        try:
            logger.info(f"Building prediction models for {player_name}")
            
            # Get correlation data (which includes merged sentiment and performance)
            correlation_result = self.correlation_engine.calculate_player_correlations(
                player_name, start_date, end_date
            )
            
            if not correlation_result['success']:
                return {
                    'success': False,
                    'error': 'Failed to get correlation data',
                    'details': correlation_result
                }
            
            # Prepare training data
            training_data = self._prepare_training_data(player_name, start_date, end_date)
            
            if training_data.empty:
                return {
                    'success': False,
                    'error': 'Insufficient training data'
                }
            
            # Build models for different performance metrics
            model_results = {}
            performance_metrics = ['points', 'rebounds', 'assists', 'plus_minus']
            
            for metric in performance_metrics:
                if metric in training_data.columns:
                    logger.info(f"Building model for {metric}")
                    
                    model_result = self._build_single_model(
                        training_data, metric, player_name
                    )
                    model_results[metric] = model_result
            
            # Generate overall insights
            insights = self._generate_prediction_insights(model_results, player_name)
            
            logger.info(f"Prediction models built for {player_name}")
            
            return {
                'success': True,
                'player_name': player_name,
                'training_data_size': len(training_data),
                'models': model_results,
                'insights': insights,
                'best_model': self._get_best_model(model_results)
            }
            
        except Exception as e:
            logger.error(f"Failed to build prediction models for {player_name}: {e}")
            return {'success': False, 'error': str(e)}
    
    def _prepare_training_data(self, player_name: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Prepare training data for machine learning models
        
        Args:
            player_name: Name of the player
            start_date: Start date
            end_date: End date
            
        Returns:
            Prepared training DataFrame
        """
        try:
            # Get sentiment data
            sentiment_data = self.correlation_engine._get_player_sentiment_data(
                player_name, start_date, end_date
            )
            
            # Get performance data
            performance_data = self.correlation_engine._get_player_performance_data(
                player_name, start_date, end_date
            )
            
            if sentiment_data.empty or performance_data.empty:
                return pd.DataFrame()
            
            # Aggregate daily sentiment
            daily_sentiment = self.correlation_engine._aggregate_daily_sentiment(sentiment_data)
            
            # Merge with performance data (use sentiment from day before game)
            performance_data['prev_day'] = performance_data['game_date'] - timedelta(days=1)
            
            merged_data = performance_data.merge(
                daily_sentiment,
                left_on='prev_day',
                right_on='date',
                how='inner'
            )
            
            if merged_data.empty:
                return pd.DataFrame()
            
            # Create feature matrix
            features = [
                'sentiment_mean', 'sentiment_std', 'sentiment_count',
                'positive_mean', 'negative_mean', 'neutral_mean',
                'total_posts'
            ]
            
            # Add rolling averages
            merged_data = merged_data.sort_values('game_date')
            for window in [3, 7, 14]:
                for feature in ['sentiment_mean', 'positive_mean', 'negative_mean']:
                    if feature in merged_data.columns:
                        merged_data[f'{feature}_rolling_{window}'] = (
                            merged_data[feature].rolling(window=window, min_periods=1).mean()
                        )
                        features.append(f'{feature}_rolling_{window}')
            
            # Add game context features
            merged_data['is_weekend'] = merged_data['game_date'].dt.weekday.isin([5, 6])
            merged_data['days_since_last_game'] = merged_data['game_date'].diff().dt.days.fillna(0)
            
            features.extend(['is_weekend', 'days_since_last_game'])
            
            # Select only available features
            available_features = [f for f in features if f in merged_data.columns]
            
            return merged_data[available_features + ['points', 'rebounds', 'assists', 'plus_minus', 'game_date']]
            
        except Exception as e:
            logger.error(f"Failed to prepare training data: {e}")
            return pd.DataFrame()
    
    def _build_single_model(self, data: pd.DataFrame, target_metric: str, 
                          player_name: str) -> Dict[str, Any]:
        """
        Build a single prediction model for a specific metric
        
        Args:
            data: Training data
            target_metric: Target performance metric
            player_name: Name of the player
            
        Returns:
            Model results dictionary
        """
        try:
            # Prepare features and target
            feature_columns = [col for col in data.columns 
                             if col not in ['points', 'rebounds', 'assists', 'plus_minus', 'game_date']]
            
            X = data[feature_columns].fillna(0)
            y = data[target_metric]
            
            if len(X) < 10:  # Need minimum data for meaningful model
                return {
                    'success': False,
                    'error': f'Insufficient data for {target_metric} model'
                }
            
            # Split data
            X_train, X_test, y_train, y_test = train_test_split(
                X, y, test_size=0.2, random_state=42
            )
            
            # Scale features
            scaler = StandardScaler()
            X_train_scaled = scaler.fit_transform(X_train)
            X_test_scaled = scaler.transform(X_test)
            
            # Train multiple models
            models = {
                'linear_regression': LinearRegression(),
                'ridge_regression': Ridge(alpha=1.0),
                'random_forest': RandomForestRegressor(n_estimators=100, random_state=42)
            }
            
            best_model = None
            best_score = -np.inf
            model_results = {}
            
            for model_name, model in models.items():
                try:
                    # Train model
                    if model_name == 'random_forest':
                        model.fit(X_train, y_train)
                        y_pred = model.predict(X_test)
                    else:
                        model.fit(X_train_scaled, y_train)
                        y_pred = model.predict(X_test_scaled)
                    
                    # Evaluate model
                    mse = mean_squared_error(y_test, y_pred)
                    r2 = r2_score(y_test, y_pred)
                    mae = mean_absolute_error(y_test, y_pred)
                    
                    # Cross-validation score
                    if model_name == 'random_forest':
                        cv_scores = cross_val_score(model, X_train, y_train, cv=3, scoring='r2')
                    else:
                        cv_scores = cross_val_score(model, X_train_scaled, y_train, cv=3, scoring='r2')
                    
                    model_results[model_name] = {
                        'mse': round(mse, 3),
                        'r2': round(r2, 3),
                        'mae': round(mae, 3),
                        'cv_mean': round(cv_scores.mean(), 3),
                        'cv_std': round(cv_scores.std(), 3),
                        'predictions': y_pred.tolist(),
                        'actual': y_test.tolist()
                    }
                    
                    # Track best model
                    if r2 > best_score:
                        best_score = r2
                        best_model = model_name
                        
                except Exception as e:
                    logger.warning(f"Failed to train {model_name} for {target_metric}: {e}")
                    model_results[model_name] = {'error': str(e)}
            
            # Store best model and scaler
            self.models[f"{player_name}_{target_metric}"] = models[best_model]
            self.scalers[f"{player_name}_{target_metric}"] = scaler
            
            return {
                'success': True,
                'target_metric': target_metric,
                'best_model': best_model,
                'best_r2': round(best_score, 3),
                'model_results': model_results,
                'feature_importance': self._get_feature_importance(
                    models[best_model], feature_columns, best_model
                ),
                'sample_size': len(data)
            }
            
        except Exception as e:
            logger.error(f"Failed to build model for {target_metric}: {e}")
            return {'success': False, 'error': str(e)}
    
    def _get_feature_importance(self, model, feature_columns: List[str], model_type: str) -> Dict[str, float]:
        """
        Get feature importance for the model
        
        Args:
            model: Trained model
            feature_columns: List of feature names
            model_type: Type of model
            
        Returns:
            Dictionary with feature importance
        """
        try:
            if model_type == 'random_forest':
                importance = model.feature_importances_
            else:
                # For linear models, use absolute coefficients
                importance = np.abs(model.coef_)
            
            # Normalize importance
            importance = importance / np.sum(importance)
            
            return dict(zip(feature_columns, importance.round(4)))
            
        except Exception as e:
            logger.warning(f"Failed to get feature importance: {e}")
            return {}
    
    def _generate_prediction_insights(self, model_results: Dict[str, Any], 
                                    player_name: str) -> List[str]:
        """
        Generate insights from prediction model results
        
        Args:
            model_results: Results from all models
            player_name: Name of the player
            
        Returns:
            List of insights
        """
        insights = []
        
        try:
            successful_models = [r for r in model_results.values() if r.get('success', False)]
            
            if not successful_models:
                return ["No successful prediction models built"]
            
            # Overall performance
            avg_r2 = np.mean([m['best_r2'] for m in successful_models])
            best_metric = max(successful_models, key=lambda x: x['best_r2'])
            
            insights.append(f"Built {len(successful_models)} prediction models for {player_name.title()}")
            insights.append(f"Average prediction accuracy (R²): {avg_r2:.3f}")
            insights.append(f"Best predicting metric: {best_metric['target_metric']} (R²={best_metric['best_r2']})")
            
            # Model performance classification
            if avg_r2 > 0.7:
                insights.append("Excellent prediction accuracy - sentiment strongly predicts performance")
            elif avg_r2 > 0.5:
                insights.append("Good prediction accuracy - sentiment moderately predicts performance")
            elif avg_r2 > 0.3:
                insights.append("Fair prediction accuracy - sentiment weakly predicts performance")
            else:
                insights.append("Poor prediction accuracy - sentiment has limited predictive value")
            
            # Feature insights
            for model_result in successful_models:
                if 'feature_importance' in model_result:
                    importance = model_result['feature_importance']
                    if importance:
                        top_feature = max(importance.items(), key=lambda x: x[1])
                        insights.append(f"Most important feature for {model_result['target_metric']}: {top_feature[0]} ({top_feature[1]:.3f})")
            
            return insights
            
        except Exception as e:
            logger.error(f"Failed to generate prediction insights: {e}")
            return [f"Error generating insights: {str(e)}"]
    
    def _get_best_model(self, model_results: Dict[str, Any]) -> Dict[str, Any]:
        """
        Get the best performing model across all metrics
        
        Args:
            model_results: Results from all models
            
        Returns:
            Best model information
        """
        try:
            successful_models = [r for r in model_results.values() if r.get('success', False)]
            
            if not successful_models:
                return {'error': 'No successful models'}
            
            best_model = max(successful_models, key=lambda x: x['best_r2'])
            
            return {
                'metric': best_model['target_metric'],
                'r2_score': best_model['best_r2'],
                'model_type': best_model['best_model'],
                'sample_size': best_model['sample_size']
            }
            
        except Exception as e:
            logger.error(f"Failed to get best model: {e}")
            return {'error': str(e)}
    
    def predict_next_game_performance(self, player_name: str, 
                                    current_sentiment: Dict[str, float]) -> Dict[str, Any]:
        """
        Predict next game performance based on current sentiment
        
        Args:
            player_name: Name of the player
            current_sentiment: Current sentiment metrics
            
        Returns:
            Prediction results
        """
        try:
            logger.info(f"Predicting next game performance for {player_name}")
            
            # Get the best model for points prediction
            model_key = f"{player_name}_points"
            
            if model_key not in self.models:
                return {
                    'success': False,
                    'error': f'No trained model found for {player_name}'
                }
            
            model = self.models[model_key]
            scaler = self.scalers[model_key]
            
            # Prepare features (this would need to match training features)
            features = np.array([
                current_sentiment.get('sentiment_mean', 0),
                current_sentiment.get('sentiment_std', 0),
                current_sentiment.get('sentiment_count', 0),
                current_sentiment.get('positive_mean', 0),
                current_sentiment.get('negative_mean', 0),
                current_sentiment.get('neutral_mean', 0),
                current_sentiment.get('total_posts', 0)
            ]).reshape(1, -1)
            
            # Scale features
            features_scaled = scaler.transform(features)
            
            # Make prediction
            prediction = model.predict(features_scaled)[0]
            
            return {
                'success': True,
                'player_name': player_name,
                'predicted_points': round(prediction, 1),
                'confidence': 'high' if abs(prediction) > 20 else 'medium',
                'sentiment_input': current_sentiment
            }
            
        except Exception as e:
            logger.error(f"Failed to predict performance for {player_name}: {e}")
            return {'success': False, 'error': str(e)}
    
    def evaluate_all_players(self, start_date: str = "2023-10-01",
                           end_date: str = "2024-05-31") -> Dict[str, Any]:
        """
        Evaluate prediction models for all qualifying players
        
        Args:
            start_date: Start date for analysis
            end_date: End date for analysis
            
        Returns:
            Evaluation results for all players
        """
        try:
            logger.info("Evaluating prediction models for all players")
            
            # Get qualifying players
            from analytics.player_mention_filter import PlayerMentionFilter
            filter_obj = PlayerMentionFilter()
            filter_result = filter_obj.get_qualifying_players(start_date, end_date)
            
            if not filter_result['success']:
                return {'success': False, 'error': 'Failed to get qualifying players'}
            
            qualifying_players = filter_result['qualifying_players']
            all_results = {}
            
            for player in qualifying_players:
                player_name = player['player_name']
                logger.info(f"Evaluating {player_name}")
                
                result = self.build_prediction_models(player_name, start_date, end_date)
                all_results[player_name] = result
            
            # Generate overall evaluation insights
            overall_insights = self._generate_evaluation_insights(all_results)
            
            return {
                'success': True,
                'players_evaluated': len(qualifying_players),
                'results': all_results,
                'overall_insights': overall_insights
            }
            
        except Exception as e:
            logger.error(f"Failed to evaluate all players: {e}")
            return {'success': False, 'error': str(e)}
    
    def _generate_evaluation_insights(self, all_results: Dict[str, Any]) -> List[str]:
        """Generate overall insights from all player evaluations"""
        insights = []
        
        try:
            successful_evaluations = [r for r in all_results.values() if r.get('success', False)]
            
            if not successful_evaluations:
                return ["No successful evaluations to generate insights"]
            
            # Calculate average performance
            avg_r2_scores = []
            for result in successful_evaluations:
                if 'best_model' in result and 'r2_score' in result['best_model']:
                    avg_r2_scores.append(result['best_model']['r2_score'])
            
            if avg_r2_scores:
                overall_avg_r2 = np.mean(avg_r2_scores)
                insights.append(f"Average prediction accuracy across all players: {overall_avg_r2:.3f}")
                
                # Performance classification
                if overall_avg_r2 > 0.6:
                    insights.append("Overall excellent prediction capability - sentiment is a strong predictor")
                elif overall_avg_r2 > 0.4:
                    insights.append("Overall good prediction capability - sentiment is a moderate predictor")
                else:
                    insights.append("Overall limited prediction capability - sentiment has weak predictive value")
            
            # Find best and worst performers
            best_player = None
            worst_player = None
            best_r2 = -1
            worst_r2 = 1
            
            for player_name, result in all_results.items():
                if result.get('success', False) and 'best_model' in result:
                    r2_score = result['best_model'].get('r2_score', 0)
                    if r2_score > best_r2:
                        best_r2 = r2_score
                        best_player = player_name
                    if r2_score < worst_r2:
                        worst_r2 = r2_score
                        worst_player = player_name
            
            if best_player:
                insights.append(f"Best prediction accuracy: {best_player.title()} (R²={best_r2:.3f})")
            if worst_player:
                insights.append(f"Worst prediction accuracy: {worst_player.title()} (R²={worst_r2:.3f})")
            
            insights.append(f"Successfully evaluated {len(successful_evaluations)} players")
            
            return insights
            
        except Exception as e:
            logger.error(f"Failed to generate evaluation insights: {e}")
            return [f"Error generating insights: {str(e)}"]


def build_prediction_models(player_name: str) -> Dict[str, Any]:
    """
    Main function to build prediction models for a player
    
    Args:
        player_name: Name of the player
        
    Returns:
        Model building results
    """
    predictor = PerformancePredictor()
    return predictor.build_prediction_models(player_name)


if __name__ == "__main__":
    # Test the performance predictor
    print("Testing Performance Predictor...")
    result = build_prediction_models("lebron_james")
    
    if result['success']:
        print("✅ Prediction models built successfully!")
        print(f"📊 Training data size: {result['training_data_size']}")
        print(f"🎯 Best model: {result['best_model']['metric']} (R²={result['best_model']['r2_score']})")
        print(f"💡 Insights: {len(result['insights'])} generated")
    else:
        print(f"❌ Model building failed: {result['error']}")
