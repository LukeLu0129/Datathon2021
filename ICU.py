
import pandas as pd
import streamlit as st
from collections import defaultdict as dd

class ICU_patient:
    
    def __init__(self, subjectID=None):
        self.sub_ID = subjectID

    @staticmethod
    def get_dir(filename = "INPUTEVENTS_MV"):
        mimic_folder_dir = "mimic-iii-clinical-database-1.4" 
        return mimic_folder_dir + '\\'+ filename + ".csv.gz"


    @staticmethod
    def findRow(TextFileReader, Col, Val, iterNum=10000, chunksize=10000, OnlyOne = False):
        '''This func iterate (default=10,000 time) over chunks (default=10,000 rows) to find the row based on specified value in a column '''
        outDF = pd.DataFrame()
        iterCount = 1
        while True:
            try:
                chunkDF = TextFileReader.get_chunk(chunksize).query(f"{Col}=={Val}")
                outDF=outDF.append(chunkDF)
                if OnlyOne and not outDF.empty:
                    print(f"Iterate file over {iterCount*chunksize} rows")
                    return outDF
                if iterCount==iterNum:
                    print(f"Iterate file over {iterCount*chunksize} rows")
                    return outDF
                iterCount+=1
            except:
                print(f"Iterate file over {iterCount*chunksize} rows")
                return outDF

    
    def findDataInFile(self, filename='', col="SUBJECT_ID", val='', **kwarg):
        if filename:
            TextFileReader = pd.read_csv(self.get_dir(filename),iterator=True)
        else:
            TextFileReader = pd.read_csv(self.get_dir(),iterator=True)
        if col=="SUBJECT_ID":
            val = self.sub_ID
        return self.findRow(TextFileReader, col ,val, **kwarg)


    @property
    def getAdmission(self):
        '''get admission data from ADMISSIONS file'''
        return self.findDataInFile('ADMISSIONS').set_index('ADMITTIME')

    @property
    def getCallout(self):
        '''get data from CALLOUT file'''
        return self.findDataInFile('CALLOUT').set_index('CREATETIME')

    @property
    def getChart(self):
        '''get data from CHARTEVENTS file'''
        return self.findDataInFile('CHARTEVENTS').set_index('CHARTTIME')

    @property
    def getStay(self):
        '''get hospitalisation duration from ICUSTAYS file'''
        return self.findDataInFile('ICUSTAYS').set_index('INTIME')

    @property
    def getDTevents(self):
        '''get events data from DATETIMEEVENTS file'''
        return self.findDataInFile('DATETIMEEVENTS').set_index('CHARTTIME')

    @property
    def getCPT(self):
        '''get data from CPTEVENTS file'''
        return self.findDataInFile('CPTEVENTS').set_index('TICKET_ID_SEQ')

    @property
    def getDiag(self):
        '''get data from DIAGNOSES_ICD file'''
        ICD9_CODE
        return self.findDataInFile('DIAGNOSES_ICD').set_index('ICD9_CODE')
    
    @property
    def getDRG(self):
        '''get data from DRGCODES file'''
        return self.findDataInFile('DRGCODES').set_index('DRG_CODE')

    @property
    def getLab(self):
        '''get data from LABEVENTS file'''
        return self.findDataInFile('LABEVENTS').set_index('CHARTTIME')

    @property
    def getMicroBio(self):
        '''get data from MICROBIOLOGYEVENTS file'''
        return self.findDataInFile('MICROBIOLOGYEVENTS').set_index('CHARTTIME') 

    @property
    def getNote(self):
        '''get data from NOTEEVENTS file'''
        return self.findDataInFile('NOTEEVENTS').set_index('CHARTDATE')    

    @property
    def getOutput(self):
        '''get data from OUTPUTEVENTS file'''
        return self.findDataInFile('OUTPUTEVENTS').set_index('CHARTTIME')  

    @property
    def getPatient(self):
        '''get demographic data from PATIENTS file'''
        return self.findDataInFile('PATIENTS', OnlyOne =True)

    @property
    def getPres(self):
        '''get data from PRESCRIPTIONS file'''
        return self.findDataInFile('PRESCRIPTIONS').set_index('STARTDATE')     

    @property
    def getProcedure(self):
        '''get data from PROCEDUREEVENTS_MV file'''
        return self.findDataInFile('PROCEDUREEVENTS_MV').set_index('STARTTIME')    
    
    @property
    def getProcedureICD(self):
        '''get data from PROCEDUREEVENTS_ICD file'''
        return self.findDataInFile('PROCEDUREEVENTS_ICD').set_index('ICD9_CODE')    

    @property
    def getService(self):
        '''get data from SERVICES file'''
        return self.findDataInFile('SERVICES').set_index('TRANSFERTIME')

    @property
    def getTransfer(self):
        '''get data from TRANSFERS file'''
        return self.findDataInFile('TRANSFERS').set_index('INTIME')    

### not used
# Patient = ICU_patient(P_ID)
# filelist = ['ADMISSIONS','CALLOUT','CAREGIVERS','CHARTEVENTS','CPTEVENTS','DATETIMEEVENTS','DIAGNOSES_ICD', 'DRGCODES',
# 'D_CPT','D_ICD_DIAGNOSES','D_ICD_PROCEDURES','D_ITEMS','D_LABITEMS','ICUSTAYS','INPUTEVENTS_CV', 'INPUTEVENTS_MV',
# 'LABEVENTS','MICROBIOLOGYEVENTS','NOTEEVENTS','OUTPUTEVENTS', 'PATIENTS', 'PRESCRIPTIONS', 'PROCEDUREEVENTS_MV','PROCEDURE_ICD',
# 'SERVICES','TRANSFERS']

def getAllOption(List):
    return sorted(set(List))

### import noteevents from SparkNLP
RaidoFile_dir = 'mimiciii-5000s-radiology-entities.parquet'
NurseFile_dir = "mimiciii-5000s-nursing-progress-notes-entities.parquet" 
DischargeFile_dir = 'mimiciii-5000s-discharge-summary-entities.parquet'
NoteFile_dir = "mimiciii-5000s-all-notes-nid.parquet" 

Radio_Note = pd.read_parquet(RaidoFile_dir)
Nurse_Note = pd.read_parquet(NurseFile_dir)
Discharge_Note = pd.read_parquet(DischargeFile_dir)
All_Note = pd.read_parquet(NoteFile_dir)

### extract filter option from column in All_Note 
# filterDict = dd(str)
# if not P_Note.empty:
#     adm_id = st.sidebar.selectbox("Filter with admission ID: ", getAllOption(P_Note.admission_id))
#     P_Note = P_Note.query('admission_id == @adm_id')
#     c_date = st.sidebar.selectbox("Filter with chart date ID: ", getAllOption(P_Note.chart_date))
#     P_Note = P_Note.query('chart_date == @c_date')
#     n_cat = st.sidebar.selectbox("Filter with note category ID: ", getAllOption(P_Note.notes_category))
#     P_Note = P_Note.query('notes_category == @n_cat')
#     n_descrip = st.sidebar.selectbox("Filter with note description ID: ", getAllOption(P_Note.notes_description))
#     P_Note = P_Note.query('notes_description == @n_descrip')

### steamlit web app design
st.sidebar.header('Clinical note search \n (powered by SparkNLP)')
sub_ID = st.sidebar.text_input("Enter the subject ID:")
keyword = st.sidebar.text_input('Enter keyword to search in clinical notes: ')

### advance setting



### concat all notes from SparkNLP
P_Note = All_Note.query("subject_id == @sub_ID")
R_Note = Radio_Note.query("subject_id == @sub_ID")
D_Note = Discharge_Note.query("subject_id == @sub_ID")
N_Note = Nurse_Note.query("subject_id == @sub_ID")
Notes = pd.concat([R_Note, D_Note, N_Note])
### search Note with regex based on chunk result
FoundNotes = Notes[Notes.chunk.str.contains(keyword, regex=True)]

Asrt = st.sidebar.multiselect('Select filters based on "assetion": ', getAllOption(FoundNotes.assertion),['Confirmed', 'Present'])
ner = st.sidebar.multiselect('Select filters based on "ner_label": ', getAllOption(FoundNotes.ner_label))
if ner:
    FoundNotes = FoundNotes.query('ner_label in @ner')
if Asrt:
    FoundNotes = FoundNotes.query('assertion in @Asrt')
NoteID = st.sidebar.selectbox('Select a Note ID to view full note',getAllOption(FoundNotes.notes_id))
if st.sidebar.button("Show FoundNote dataframe"):
    st.dataframe(FoundNotes, width=5000,height=5000)
else:
    st.pl
    st.text(All_Note.query(f'notes_id== "{NoteID}"').notes_text.values[0])


# 
# admID = st.sidebar.text_input("Enter an admission ID")
# kw = st.sidebar.text_input("Enter keyword to search in radiology related ")
# A = Radio_Note.query(f'admission_id == "{admID}"').query('assertion == "Confirmed"').query('chunk==@kw').notes_id
# # st.dataframe(Patient.findDataInFile(filename), width=5000)
# # st.dataframe(A, width=5000)
# if not A.empty:
#     note_id = st.sidebar.selectbox(f'{kw} is found in the notes list, select a note ID to see full notes', sorted(set(A)))
    
#     st.text(All_Note.query(f'notes_id== "{note_id}"').notes_text.values[0])