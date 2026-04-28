------
**Final Project**

This is an individual project. This project is meant to assess ability to use spark to handle streaming
data, use spark for fitting a machine learning model, and a few other things we’ve talked about in class (and
a few we haven’t!)

**Goals**

For this project, we’ll
• create or use an already-created GitHub repo to document your work
– You must commit often! At least 5 substantial commits must exist on this project. This allows
us to see how your work develops over time. If this is not done, substantial credit will be lost.
• write a Jupyter notebook that fits a machine learning model using pyspark’s MLlib module
• in that same notebook you’ll write code to read in a stream of data (we’ll produce that data ourselves
using a .py file)
– This .py file should also be included in the repo.
• we’ll use the model to do predictions on the stream and write those out to the console!
Both the .ipynb file and the .py file should exist in your repo!
The data is modified from the UCI machine learning repository. The study was about relating power
consumption from different zones of Tetouan city to various factors like time of day, temperature, and
humidity.
• You’ll have a chunk of the (modified) data to build your model on.
• You will then ‘stream data’ to a folder in the form of CSV files. You’ll be monitoring this folder. When
data comes in you’ll use your fitted model to predict on the incoming data!
