'''
These functions download relevant lodes files.
'''

from bs4 import BeautifulSoup
import requests
import pickle
import os
import time 
import gzip
import glob

def get_all_possible_files(
        save: bool = False, 
        savepath : str = '',
        savename : str = '') -> dict:
    
    '''
    This function creates a dictionary of all possible files in the LODES 8 directory, plus the crosswalk file.
    :param bool save: If true, this will save the dictionary as a Pickle file in a given directory.
    :param str savepath: Path to save output file 
    :param str savename: Name to save output pickle file
   '''
    url = r"https://lehd.ces.census.gov/data/lodes/LODES8/"
    reqs = requests.get(url)
    soup = BeautifulSoup(reqs.text, 'html.parser')

    #get all state level URLs
    urls = []
    for link in soup.find_all('a'):
        try:
            if len(link.get('href')) < 4:
                urls.append(r"https://lehd.ces.census.gov/data/lodes/LODES8/" + link.get('href'))
        except:
            continue

    #make a dictionary with all the relevant zip files for each state
    f_st = {}
    print("getting each state's potential files...")
    for q in urls:
        st = q.split("/")[-2]
        if len(st) < 3:
            #print(f'{st}...')
            w_d = {}
            #go to each states od, rac, and wac page
            for z in ['od','rac','wac']:
                try:
                    l = q + z
                    reqs = requests.get(l)
                    soup = BeautifulSoup(reqs.text, 'html.parser')
                    t_urls = []
                    for link in soup.find_all('a'):
                        try:
                            fl = f"{l}/" + link.get('href')
                            if fl.endswith('.gz'):
                                t_urls.append(fl)
                        except:
                            continue
                    w_d[z] = t_urls
                except:
                    continue
            #add in crosswalk
            w_d['cw'] = [q + f"{st}_xwalk.csv.gz"]
            f_st[st] = w_d
    print(f'done')
    
    #optional save dictionary step
    if save == True:
        try:
            print("saving the dictionary to a pickle file...")
            with open(os.path.join(savepath,f"{savename}.pkl"),'wb') as fp:
                pickle.dump(f_st, fp)
                print('dictionary saved successfully to file...')
                print(f'saved at: {os.path.join(savepath,f"{savename}.pkl")}')
        except Exception as e:
            print(e)
            
    print("done!")
    return f_st

def load_existing_state_dict(
        file_path : str) -> dict:
    '''
    read in an existing LODES file directory dictionary, which is the output of the get_all_possible_files() function
    :param str file_path: file where you have the dictionary as a pickle (hint: output of get_all_possible_files())
    '''

    import pickle

    print(f"loading dict at: {file_path}")
    try:
        with open(file_path, 'rb') as handle:
            b = pickle.load(handle)
        return b 
    except:
        print("could not load state dict ooooops")

def download_state_lodes_file(save_loc: str, 
                              st: str,
                              links_dict: dict,
                              skip_existing: bool = True) -> str:
                                  
    '''
    download a single state's full lodes file to a specific folder.
    returns a string with a path to a folder.
    :param str save_loc: path to a folder where you'll save output files
    :param str state: two letter state code for the state you want to download
    :param dict links_dict: dictionary you want with links (hint: output of get_all_possible_files())
    '''
    
    #prepare folders for output
    try:
        links = links_dict[st]
    except:
        print("error with state code; not in dict")

    #make overarching folder
    fold = os.path.join(save_loc,st)
    if not os.path.exists(fold):
        os.makedirs(fold)

    #make a subfolder for each od/rac/wac
    for s in ['od','rac','wac','cw']:
        fold2 = os.path.join(fold,s)
        if not os.path.exists(fold2):
            os.makedirs(fold2)
    
    #loop through and download all files
    start = time.strftime("%H:%M:%S")
    skipcounter = 0
    print(f"start time: {start}")
    for s in ['od','rac','wac','cw']:
        print(f"{st} + {s}...")
        try:
            urls = links[s]
            
            counter = len(urls)
            print(f"downloading {counter} {s} files")
            for i,zurl in enumerate(urls):

                #print update every 25 files
                if (i % 50 == 0) or (i+1 == counter):
                    print(f"{((i+1)/counter):.1%} complete...")
                    if(skipcounter > 0):
                        print("Skipped {} files that already existed".format(skipcounter))
                
                #make the output path
                save_location = os.path.join(f"{fold}\\{s}",zurl.split("/")[-1])
                if(os.path.exists(save_location)):
                    if(skip_existing == True):
                        skipcounter = skipcounter + 1
                        continue
                # Make an HTTP GET request to download the .zip file
                response = requests.get(zurl)
                if response.status_code == 200:
                    # Open the file in binary write mode and save the content
                    with open(save_location, 'wb') as file:
                        file.write(response.content)
                else:
                    print(f"failed to download")
        except:
            continue
    print("done downloading!")
    end = time.strftime("%H:%M:%S")
    if(skipcounter > 0):
        print("Skipped {} files that already existed".format(skipcounter))
    print(f"end time: {end}")
    return fold

def unzip_state_lodes_file(state_fold : str = None, skip_existing : bool = True, delete_corrupt : bool = True) -> str:
    '''
    unzip a state's lodes data, using the parent folder location with the data

    :param str state_fold: folder with all the data; this is the output of the download state lodes file
    '''
    

    # Define the path to the subfolder containing the .gz files
    paths = [q for q in glob.glob(state_fold+"\\**",recursive=True) if q.endswith(('rac','wac','od','cw'))]

    start = time.strftime("%H:%M:%S")
    print(f"start time: {start}")


    for sub_path in paths:
        try:
            # Get a list of all files in the subfolder
            file_list = os.listdir(sub_path)

            counter = len(file_list)
            ty = sub_path.split('\\')[-1]
            print(f"unzipping {counter} {ty} files")

            # Iterate through the files in the subfolder
            skipcounter = 0
            for i, filename in enumerate(file_list):
                
                if (i % 50 == 0) or (i+1 == counter):
                    print(f"{((i+1)/counter):.1%} complete...")
                    if(skipcounter > 0):
                        print("Skipped {} files that already existed".format(skipcounter))
                        

                # Check if the file has a .gz extension
                if filename.endswith('.gz'):
                    # Construct the full path to the .gz file
                    gz_file_path = os.path.join(sub_path, filename)

                    # Remove the .gz extension to get the output filename
                    output_filename = filename[:-3]

                    # Construct the full path to the output file
                    output_file_path = os.path.join(sub_path, output_filename)

                    if(skip_existing == True):
                        if(os.path.exists(output_file_path)):
                            skipcounter = skipcounter + 1
                            continue
                    try:
                        # Open the .gz file for reading and the output file for writing
                        with gzip.open(gz_file_path, 'rb') as gz_file, open(output_file_path, 'wb') as out_file:
                            # Read and write the contents to decompress
                            out_file.write(gz_file.read())
                    except Exception as e:
                        print(f"error unzipping {filename}: {str(e)}")
                        if(delete_corrupt == True):
                            print("Deleting corrupt gzip file {}".format(gz_file_path))
                            #os.unlink(gz_file_path)
        except:
            continue
    end = time.strftime("%H:%M:%S")
    print(f"end time: {end}")
    if(skipcounter > 0):
        print("Skipped {} files that already existed".format(skipcounter))


