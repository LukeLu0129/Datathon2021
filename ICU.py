
import pandas as pd
import streamlit as st
from collections import defaultdict as dd


def getAllOption(List):
    return sorted(set(List))

### import noteevents from SparkNLP
RaidoFile_dir = 'mimiciii-5000s-radiology-entities.parquet'
NoteFile_dir = "mimiciii-5000s-all-notes-nid.parquet" 
Radio_Note = pd.read_parquet(RaidoFile_dir)
All_Note = pd.read_parquet(NoteFile_dir)
Notes = Radio_Note
AIS_dict ={'subdural hemorrhage':4, 'facial bone fractures':4, 
'subdural hematoma':4, 'nondisplaced fractures': 4, 'occipital fractures':4, 
'subarachnoid hemorrhage':5,'diffuse axonal injury':5, 'mass effect on the cerebellum':5}

### steamlit web app design
st.sidebar.header('Clinical note search \n (powered by SparkNLP)')
col1 = 'notes_description' #st.sidebar.selectbox('Select a column to search from: ', getAllOption(Notes.columns))
keyword1 = st.sidebar.text_input('Enter keyword to search in notes description: ')

col2 = st.sidebar.selectbox('Select a column to search from: ', getAllOption(Notes.columns.drop('notes_description')))
keyword2 = st.sidebar.selectbox('Select a keyword: ',AIS_dict.keys())

if keyword1:
    Notes = Notes[Notes[col1].str.contains(keyword1, case=False)]
if keyword2:
    Notes = Notes[Notes[col2].str.contains(keyword2, case=False)]

if st.sidebar.button('show dataframe'):
    st.dataframe(Notes.head(100), width=5000,height=5000)

noteID = st.sidebar.selectbox('Select a note id to view full notes ', getAllOption(Notes.notes_id))
st.title(f'Estimated AIS = {AIS_dict[keyword2]}')
st.text(All_Note.query(f'notes_id == "{noteID}"').notes_text.iloc[0])