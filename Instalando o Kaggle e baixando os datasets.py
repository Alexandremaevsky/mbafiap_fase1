
# coding: utf-8

# 1 - Instalando o Kaggle e baixando o Arquivo de credenciais do Kaggle
# 
# Vá até o site do Kaggle e crie sua conta.
# https://www.kaggle.com. Em seguida, vá para a aba Account do seu perfil de usuário e selecione Create API Token.
# 
# Isso fará o download de kaggle.json, um arquivo que contém suas credenciais de API.
# 
# Salve o arquivo na pasta /mnt/notebooks

# In[4]:


get_ipython().system(' pip install kaggle')


# 2 -Download dos datasets do olist para disco local

# In[5]:


import os
import shutil


# In[2]:


if not os.path.exists(r'/root/.kaggle'):
    os.makedirs(r'/root/.kaggle')
    
os.chdir(r'/root/.kaggle') 
    
os.getcwd()


# In[9]:


os.chdir(r'/mnt/notebooks') 

os.getcwd()


# In[12]:


if not os.path.exists(r'/mnt/notebooks/grupob'):
    os.makedirs(r'/mnt/notebooks/grupob') 
    os.makedirs(r'/mnt/notebooks/grupob/olist') 


# In[17]:


os.chdir(r'/mnt/notebooks') 

shutil.copy('kaggle.json', '/root/.kaggle/')

os.chdir(r'/root/.kaggle')

os.getcwd()


# In[18]:


if not os.path.exists(r'/mnt/notebooks/grupob'):
    os.makedirs(r'/mnt/notebooks/grupob') 
    os.makedirs(r'/mnt/notebooks/grupob/olist') 


# In[19]:


os.chdir(r'/mnt/notebooks/grupob/olist') 

os.getcwd()


# In[21]:


get_ipython().system(' kaggle datasets download -d olistbr/brazilian-ecommerce')


# In[23]:


import zipfile
with zipfile.ZipFile("/mnt/notebooks/grupob/olist/brazilian-ecommerce.zip", 'r') as zip_ref:
    zip_ref.extractall('/mnt/notebooks/grupob/olist')


# 3- Verificando se os datasets foram baixandos para a pasta

# In[24]:


os.chdir(r'/mnt/notebooks/grupob/olist') 

get_ipython().system(' rm -f *.zip')

get_ipython().system(' ls /mnt/notebooks/grupob/olist')


# 4- Copiar para o HDFS os datasets e listar

# In[25]:


get_ipython().system(' hadoop fs -put * /datalake/olist/raw/csv')

get_ipython().system(' hadoop fs -ls /datalake/olist/raw/csv')

