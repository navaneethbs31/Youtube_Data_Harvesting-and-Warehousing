# Youtube_Data_Harvesting-and-Warehousing using SQL and Streamlit
Domain : Social Media

YouTube Data Harvesting and Warehousing is a project that intends to provide users with the ability to access and analyse data from numerous YouTube channels. SQL, MongoDB, and Streamlit are used in the project to develop a user-friendly application that allows users to retrieve, save, and query YouTube channel and video data.

Problem Statement: The problem statement is to create a Streamlit application that allows users to access
and analyze data from multiple YouTube channels.Extracting data using Youtube API and transferring it to a relational databaselike MySQL for getting various info about youtube channels.

Overview:
This project aims to develop a user-friendly Streamlit application that utilizes the Google API to extract information on a YouTube channel, stores it in a MongoDB database, migrates it to a SQL data warehouse, and enables users to query and view data in the Streamlit app.

Tools used:

Streamlit : Streamlit library was used to create a user-friendly UI that enables users to interact with the programme and carry out data retrieval and analysis 
            operations.

Python : Python is a powerful programming language renowned for being easy to learn and understand. Python is the primary language employed in this project for the 
         development of the complete application, including data retrieval, processing, analysis, and visualisation.

Google API client : The googleapiclient library in Python facilitates the communication with different Google APIs. Its primary purpose in this project is to       
                    interact with YouTube's Data API v3, allowing the retrieval of essential information like channel details, video specifics, and comments. By 
                    utilizing googleapiclient, developers can easily access and manipulate YouTube's extensive data resources through code.

MongoDB : MongoDB is built on a scale-out architecture that has become popular with developers of all kinds for developing scalable applications with evolving data 
          schemas. As a document database, MongoDB makes it easy for developers to store structured or unstructured data.

MySQL : MySQL is a powerful and versatile database management system that is suitable for a wide range of applications, from small-scale websites and applications 
        to large-scale enterprise solutions.MySQL is an open-source relational database management system (RDBMS) that is widely used for building scalable and 
        high-performance database-driven applications. Its combination of performance, reliability, and ease of use makes it a popular choice for developers and 
        organizations worldwide.

Pandas : pandas is a fast, powerful, flexible and easy to use open source data analysis and manipulation tool, built on top of the Python programming language.


Libraries used:

1. googleapiclient.discovery
2. pandas
3. streamlit
4. re
5. mysql.connector
6. pymongo

Features : The following functions are available in the YouTube Data Harvesting and Warehousing application:

1. Retrieval of channel and video data from YouTube using the YouTube API.
2. Storage of retrieved data in a MongoDB database as a data lake.
3. Migration of data from the data lake to a SQL database for efficient querying and analysis.
4. Search and view data stored in SQL database using Streamlit.
