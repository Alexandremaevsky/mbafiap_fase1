
# coding: utf-8

# In[1]:


import os
import shutil


# In[2]:


hadoop fs -ls /


# In[3]:


get_ipython().system(' hadoop fs -ls /')


# In[4]:


get_ipython().system(' hadoop fs -mkdir /datalake')
get_ipython().system(' hadoop fs -mkdir /datalake/olist')
get_ipython().system(' hadoop fs -mkdir /datalake/olist/raw')
get_ipython().system(' hadoop fs -mkdir /datalake/olist/raw/csv')
get_ipython().system(' hadoop fs -mkdir /datalake/olist/cleansed')
get_ipython().system(' hadoop fs -mkdir /datalake/olist/enriched')
get_ipython().system(' hadoop fs -mkdir /datalake/olist/consumption')
get_ipython().system(' hadoop fs -mkdir /datalake/olist/sandbox')
get_ipython().system(' hadoop fs -mkdir /datalake/olist/experiment')
get_ipython().system(' hadoop fs -mkdir /datalake/olist/trusted')


# In[5]:


get_ipython().system(' hadoop fs -ls /')


# In[6]:


get_ipython().system(' hadoop fs -ls /datalake/olist')

