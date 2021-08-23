from delphi_epidata.requests import CovidcastEpidata, EpiRange

epidata = CovidcastEpidata("test")
print(list(epidata.source_names))
apicall = epidata[("fb-survey", "smoothed_cli")].call(
    "nation",
    "us",
    EpiRange(20210405, 20210410),
)
print(apicall)

classic = apicall.classic()
print(classic)

r = apicall.csv()
print(r[0:100])

data = apicall.json()
print(data[0])

df = apicall.df()
print(df.columns)
print(df.dtypes)
print(df.iloc[0])
df = apicall.df(disable_date_parsing=True)
print(df.columns)
print(df.dtypes)
print(df.iloc[0])

for row in apicall.iter():
    print(row)
