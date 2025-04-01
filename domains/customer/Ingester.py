
from Reader import read_data

def populate_nces_data(focus_data, sf_data):
    # for r in focusList[]:
#         ncesID = ''
#         necsData = []
#         if r.DistrictName in fuzzyMatchWithSF(r.DistrictName)
#             ncesID = takeNCESIDFromSF(r.DistrictName)
#         else
#             ncesID = fuzzyMatchWithNCES(r.DistrictName)
#         end
#          ncesData = takeNCESData(ncesID)
#     end

#     for nonMatches in FocusList[]:
#         keepit as such with NCES info

#     for nonMatches in SF with FocusList[]:
#         goto DataStewards
    pass

def validate_focus_data(focus_data):
    # completeness of data, 
    # identify incorrectness,
    # review data quality, 
    # validate data fields,
    # categorize issues. 
    pass

def process_data():
    focus_file = 'DataFiles/FOCUS_SCHOOLS_DISTRICTS.csv'
    focus_data = read_data( focus_file)
    sf_file = 'DataFiles/SF_ACCOUNTS.csv'
    sf_data = read_data( sf_file)
    validated_focus_data = validate_focus_data(focus_data)
    populate_nces_data(validated_focus_data, sf_data)

def main():
    process_data()

if __name__ == "__main__":
    main()


