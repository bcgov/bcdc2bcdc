
import  constants
import concurrent.futures
import itertools


pkgList = ["aircraft-emissions-2000-5-km-grid",
    "airfields-trim-enhanced-base-map-ebm",
    "airphoto-centroids",
    "airphoto-flightlines",
    "air-photo-system-air-photo-polygons",
    "air-photo-system-air-photo-polygons-spatial-view",
    "air-photo-system-centre-points"]


def getTasksToDo():


with concurrent.futures.ThreadPoolExecutor() as executor:

    # Schedule the first N futures.  We don't want to schedule them all
    # at once, to avoid consuming excessive amounts of memory.
    futures = {
        executor.submit(perform, task)
        for task in itertools.islice(tasks_to_do, HOW_MANY_TASKS_AT_ONCE)
    }

    while futures:
        # Wait for the next future to complete.
        done, futures = concurrent.futures.wait(
            futures, return_when=concurrent.futures.FIRST_COMPLETED
        )

        for fut in done:
            print(f"The outcome is {fut.result()}")

        # Schedule the next set of futures.  We don't want more than N futures
        # in the pool at a time, to keep memory consumption down.
        for task in itertools.islice(tasks_to_do, len(done)):
            futures.add(
                executor.submit(perform, task)
            )




url = os.environ[constants.CKAN_URL_DEST]
apiKey = os.environ[constants.CKAN_APIKEY_DEST]
packageShow = 'api/3/action/package_show'
pkgShowUrl = f"{url}/{packageShow}"

print(f'pkgShowUrl: {pkgShowUrl}')

from concurrent.futures import as_completed
from requests_futures.sessions import FuturesSession
with FuturesSession() as session:
    futures = []
    # loading up the stack of requests
    for pkgName in pkgList:
        pkgShowParams = {'id': pkgName}
        pkgShowRequest = session.get(pkgShowUrl, params=pkgShowParams)
        futures.append(pkgShowRequest)

    # dealing with the requests
    for future in as_completed(futures):
        resp = future.result()
        pkgData = resp.json()


        print(f"pkgName: {pkgData['result']['name']}, pkgid: {pkgData['result']['id']}")
