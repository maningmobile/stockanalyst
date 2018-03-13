import matplotlib.pyplot as plt  # Import matplotlib
# This line is necessary for the plot to appear in a Jupyter notebook
% matplotlib
inline
# Control the default size of figures in this Jupyter notebook
% pylab
inline
pylab.rcParams['figure.figsize'] = (15, 9)  # Change the size of plots

apple["Adj Close"].plot(grid=True)  # Plot the adjusted closing price of AAPL