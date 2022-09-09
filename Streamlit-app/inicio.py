import streamlit as st
import pandas as pd
import datetime as dt
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import lib.graficos as graficos 



st.set_page_config(
     page_title="Introduccion",
     page_icon="e88a",
     layout="wide",
     initial_sidebar_state= "collapsed",
     
)
def local_css(file_name):
    with open(file_name) as f:
        st.markdown('<style>{}</style>'.format(f.read()), unsafe_allow_html=True)

#local_css("style.css")


def main():

    st.sidebar.write('Para una correcta visualización, utilizar modo "Light". (Menu derecho-superior/ Settings/ Theme Choose: Light)')
    

    st.image('./images/ComisionLat.png')
    #st.image('./images/kpi6_1.png')




if __name__ == '__main__':
    main()
