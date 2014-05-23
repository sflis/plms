import os


#function to create directory that check if the directory alread have been created.
def ensure_dir(f):
    d = os.path.dirname(f)
    #print(d)
    if not os.path.exists(d):
        os.makedirs(d)
#===================================================================================================	   
#Simple stupid parser....
def parse(string_list, parse_string, n = 0, separator=':',complete_line = False):

    for line in string_list:
        line = line.split(separator)

        if(line[0] == parse_string):
            if(complete_line):
                return line[1].split()
            else:
                el = line[1].split()
                return el[n]

#=====================================================================================================

              