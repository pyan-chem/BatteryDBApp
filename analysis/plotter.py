import matplotlib.pyplot as plt
import logging

# Get logger for this module
logger = logging.getLogger(__name__)


class DataPlotter:
    @staticmethod
    def plot_data(df, x_col='time', y_col='voltage', title="Battery Data"):
        """Plot battery data using matplotlib.
        
        Args:
            df: DataFrame containing the data to plot
            x_col: Column name for x-axis (default: 'time')
            y_col: Column name for y-axis (default: 'voltage')
            title: Title for the plot (default: "Battery Data")
        """
        if df is None or df.empty:
            logger.warning(f"Cannot plot: DataFrame is None or empty")
            print("No data to plot")
            return
            
        try:
            logger.debug(f"Creating plot: x_col='{x_col}', y_col='{y_col}', title='{title}'")
            
            if x_col in df.columns and y_col in df.columns:
                plt.figure(figsize=(10, 6))
                plt.plot(df[x_col], df[y_col], label=y_col)
                plt.xlabel(x_col)
                plt.ylabel(y_col)
                plt.title(title)
                plt.legend()
                plt.grid(True)
                logger.info(f"Plot created successfully with {len(df)} data points")
                plt.show()
            else:
                logger.error(f"Columns '{x_col}' or '{y_col}' not found in DataFrame. Available columns: {list(df.columns)}")
                print(f"Columns {x_col} or {y_col} not found in data.")
        except Exception as e:
            logger.error(f"Error plotting data: {e}", exc_info=True)
            raise
