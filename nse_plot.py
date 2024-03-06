import numpy as np
import scipy.special
import pandas as pd
from bokeh.layouts import gridplot
from bokeh.palettes import HighContrast3
from bokeh.plotting import figure, show
from bokeh.models import CrosshairTool, HoverTool
from bokeh.io import show, output_notebook


def generate_candle_histogram(df, bins):
    """
    Generates a Bokeh histogram plot for candle sizes (close - open) along with the PDF.

    Args:
        df (pd.DataFrame): DataFrame containing OHLC candle data with columns 'date', 'open', 'high', 'low', 'close'.

    Returns:
        None (Displays the Bokeh plot)
    """

    output_notebook()
    # Set up Bokeh tools and figure
    TOOLS = "pan,wheel_zoom,box_zoom,reset,save,crosshair"  # Added crosshair tool
    p = figure(x_axis_type="datetime", tools=TOOLS, width=1000, title="Candle Size Histogram")
    p = figure(tools=TOOLS, width=1000, title="Candle Size Histogram")

    # Create histogram
    hist, edges = np.histogram(df["size_pc"], bins=bins, density=True)
    p.quad(top=hist, bottom=0, left=edges[:-1], right=edges[1:], fill_color="blue", line_color="white", alpha=0.7)

    # Calculate PDF (Probability Density Function)
    x = np.linspace(df["size_pc"].min(), df["size_pc"].max(), 1000)
    pdf = 1 / (np.std(df["size_pc"]) * np.sqrt(2 * np.pi)) * np.exp(-(x - np.mean(df["size_pc"])) ** 2 / (2 * np.std(df["size_pc"]) ** 2))

    # Plot PDF
    p.line(x, pdf, line_color="red", line_width=2, legend_label="PDF")

    # Customize plot
    p.xaxis.major_label_orientation = 0.8  # Rotate x-axis labels
    p.grid.grid_line_alpha = 0.3

    # Add crosshair tool for both axes
    p.add_tools(CrosshairTool(dimensions="both"))

    # Add tooltips
    hover = HoverTool()
    hover.tooltips = [("Candle Size", "$x{0.00}"), ("PDF", "$y{0.000}")]
    p.add_tools(hover)

    # Show the plot
    show(p)

# Example usage:
# Load your own candle data into a DataFrame (replace this with your data)
# df = pd.read_csv("your_candle_data.csv")
# generate_candle_histogram(df)

