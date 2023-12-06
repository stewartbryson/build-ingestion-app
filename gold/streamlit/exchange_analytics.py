# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.types import *
from snowflake.snowpark.functions import *

# Write directly to the app
st.title("Fifty Two Week Analysis")
st.write(
   """How are exchanges performing over fifty-two weeks?
   """
)

# Get the current credentials
session = get_active_session()

#  Create an example data frame
data_frame = (
    session.sql("SELECT * from published.rep_exchange")
    #.where(col('fifty_two_week_high_rank') < 10)
    )

# Execute the query and convert it into a Pandas data frame
queried_data = data_frame.to_pandas()

# Display the Pandas data frame as a Streamlit data frame.
# st.dataframe(queried_data, use_container_width=True)
st.subheader("Exchange Performance")
st.bar_chart(data=queried_data, x="EXCHANGE_ID", y=["FIFTY_TWO_WEEK_HIGH_AVG","FIFTY_TWO_WEEK_LOW_AVG"])
st.dataframe(queried_data, use_container_width=True)