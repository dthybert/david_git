
from typing import List, Dict
import subprocess
import os
import argparse


def get_list_pipelines(keyword: str, dbc_search: str) -> List:
    """Returns the list of tuple (server db, pipeline db) for the pipelines instance corresponding to the keyword

		Args:
			keyword: keyword for the pipeline instance search.
	"""

    result = []
    command = dbc_search + " " + keyword
    print(command)
    # command = "ls -l"
    out_process = subprocess.check_output(command, shell=True)
    output = out_process.decode("utf-8").split("\n")
    for line in output:
        if line != "":
            tab = line.split()
            result.append((tab[0], tab[1]))
    return result


def get_list_url(pipelines: List, dbc_url: str) -> List:
    """Returns the list of urls associated to the pipeline instances

	Args:
		pipelines: list of tuples

	"""

    result = []
    for pipeline in pipelines:
        command = dbc_url + " " + pipeline[0] + " " + pipeline[1]
        print(command)
        out_process = subprocess.check_output(command, shell=True)
        output = out_process.decode("utf-8")
        result.append(output.strip())
    return result


def time_line(urls: List, mode: str, out_dir: str) -> None:
    """Run the timeline script for each pipeline url provided in argument

		Args:
			urls: list of pipeline url
			out_dir: location where to store all time line image
	"""

    for url in urls:
        # building the command
        db_name = url.split("/")[-1]
        out_file = out_dir + "/" + db_name + ".tsv"
        command = "generate_timeline.pl" + " -url " + url + " -mode " + mode + " > " + out_file
        print(command)
        out_process = subprocess.run(command, shell=True)


def get_unitary_results(file: str, tl: Dict):
    """Retrieve the results of a file and integrate it into the timeline dictionary

        Args:
			file: file path pointing to the time line data
			tl: time line dicrtionary storing all time line data.
    """
    print(file)
    with open(file) as file_handler:
        for line in file_handler:
            if "date" in line:
                continue
            tab = line.split()
            if len(tab) < 2: # this case happen some tine and no timeline is available so total is not filled
                continue
            if not tab[0] in tl:
                tl[tab[0]] = 0.0
            if not tab[1] == "NA":
                tl[tab[0]] = tl[tab[0]] + float(tab[1])
    return tl


def integrate_results(dir: str, mode: str, out_file: str) -> None:
    """Merge the result of all pipeline timeline into one file and sum all value per unit of time

           Args:
               dir: directory with all the pipeline timeline out file
               mode: running mode of the timeline script
               out_file: location of the file reporting the merged pipeline results.
    """
    files = os.listdir(dir)
    time_result = {}
    # integrate rsults from all files
    for file in files:
        complete_file = dir + "/" + file
        time_result = get_unitary_results(complete_file, time_result)

    keys = list(time_result.keys())
    sorted_keys = sorted(keys)

    # write the output result
    max_value = 0
    with open(out_file, "w") as file_handler:
        file_handler.write("time\t"+mode+"\n")
        for key in sorted_keys:
            if key == "total" or key == "proportion" or key == "cum_proportion" :
                continue
            if max_value < time_result[key]:
                max_value = time_result[key]
            file_handler.write(key + "\t" + str(time_result[key]) + "\n")
    print("total " + mode + " is: " + str(time_result["total"]) )
    print ("the maximum " + mode + " required in one minute is " + str(max_value))


def main(keyword: str, mode: str, out_dir: str, out_file: str) -> None:
    """ Main function of the script """

    dbc_search = "./dbc_search"
    dbc_url = "./dbc_url"

    # get the list of pipelines according to the keyword
    pipelines = get_list_pipelines(keyword, dbc_search)

    # get the list of pipeline urls
    urls = get_list_url(pipelines, dbc_url)

    # run the timeline for each pipelines
    time_line(urls, mode, out_dir)

    # sum the result of the output of all pipelines
    integrate_results(out_dir, mode, out_file)
    print("pipeline run correctly")


##################################################################


parser = argparse.ArgumentParser(description='Run the timeline script on pipelines corresponding to a keyword')
parser.add_argument('--keyword', type=str, help='keyword for searching pipeline instance')
parser.add_argument('--mode', type=str, help='running mode [pending_workers, memory, workers, pending_time, cores]',
                    default="workers")
parser.add_argument('--out_dir', type=str, help='outDirectory where to store the timeline data')
parser.add_argument('--out_file', type=str, help='out file store the time line report')
args = parser.parse_args()

main(args.keyword, args.mode, args.out_dir, args.out_file)
