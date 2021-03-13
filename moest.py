import pandas as pd

data = {
    "calories": [420, 380, 3],
    "f": [0,0,0],
    "duration": [4, 5, 3],
    "type": ["duration","calories", "duration"]
}

def get(s):
    if s == "duration":
        return 3
    return 5
#load data into a DataFrame object:
df = pd.DataFrame(data)
filter_chan = df['calories'] % 2 == 0
df['duration'] = df['duration'] + df['duration']*(1+ df['duration'])/2
df.loc[filter_chan,'calories'] = df.loc[filter_chan,'calories']/2
df.loc[~filter_chan,'calories'] = df.loc[~filter_chan,'calories']*2
# df.loc['f'] = df.loc[df.loc[df['type']]]
# df['age'].apply(lambda x: 'Adult' if x>=18 else 'Child')
df.loc[filter_chan,'f'] = df.apply(lambda x: 3 if x['type'] == "duration" else 5, axis=1)
print(df)
