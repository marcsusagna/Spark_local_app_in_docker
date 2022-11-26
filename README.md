# Spark local app running on Docker

Solution file with the top 10 tracks is under folder "solution".

## How to execute the app

Steps: 
1. Install docker in your machine
2. Change your working directory to desired location where you want to bring the repository.
3. Clone the repository to your working directory.

git clone https://github.com/marcsusagna/Spark_local_app_in_docker.git

4. Change working directory within the repository by:

cd Spark_local_app_in_docker

5. Get the data: 
   1. Go to http://ocelma.net/MusicRecommendationDataset/lastfm-1K.html
   2. Download lastfm-dataset-1K.tar.gz
   3. Move file into the folder ./data (just created when cloning the repo) from your current working directory
   4. Decompress into the same ./data folder

6. Open up a shell and run the next steps 
7. Build the docker image with:

docker image build -t spark_local_app:0.1.1 ./

8. Run a container based on the image with the following command. 
**Important** replace {abs/path/to/working_dir/} with the working directory defined in step 2

docker run -dit --name my_spark_container -v {abs/path/to/working_dir/}:/spark_app/ spark_local_app:0.1.1
9. Run unit tests by executing:

docker exec my_spark_container pytest

10. Run data health checks to validate data assumptions by executing: Feel free to change spark parameters depending
on the machine you are going to run this

docker exec my_spark_container spark-submit --master local[4] --executor-memory 2g ./data_health_checks_main.py  

11. Finally, execute the application solving the required task: Feel free to change spark parameters depending
on the machine you are going to run this

docker exec my_spark_container spark-submit --master local[4] --executor-memory 2g ./main.py

12. As a result, you'll have in the folder output two files
    1. A folder called top_tracks with a csv inside with the answer
    2. a .txt file with the query plan of to obtain such result

## Assumptions on the task content

The following assumptions have been taken based on the data and requirements:
- Since track_id is null for 11% of the entries, 
the concatenation of artist name and track name is used to uniquely identify a track
(since two artist can name their songs the same).
- Ties don't matter: If two sessions happen to have the same length and they happen to be the 20th session, 
then one of them is taking arbitrarily. If ties want to be kept, a solution based on running F.rank()
on an empty Window on the session length dataset. Note that this decision can change the final result.
The current solution may lead to non-deterministic result at expense of better performance since
orderBy + limit does precomputations before shuffling (like a combiner in MapReduce), instead a Window functions shuffles all data first.
- Length of a session is defined by total number of tracks played, not unique tracks played

## Data assumptions for the proposed solution

The assumptions before are conceptual assumptions to deliver the solution. These assumptions are
data quality assumptions required in order to either be able to run the transform in a way such that results
are correct. 

This has been done thinking as if this transform would be part of a data pipeline repeating every x time. 
In that case, we would need to validate the data quality assumptions before executing the transform. 
That's why in the execution phase there is step 9 (even though for a one-off analysis would not be required
since one knows what assumptions are violated thanks to exploratory data analysis).

### Data assumptions: 

1. Fields user_id and track_start_timestamp can't be null: If they were null it would create meaningless window functions.
That's why on the data health run we would except if that's the case.
2. Column track_start_timestamp is correctly formatted: Otherwise we would have nulls when parsing it from string
and we those rows would be attached to the last session for the user. Since this would lead to incorrect results,
we raise an exception 
3. Combination of user_id and track_start_timestamp should be unique. This doesn't invalidate the solution, 
as the two plays would be considered part of the same session. However, from a conceptual point of view
one would expect them to constitute a PK (together with assumption 1). In fact, in this dataset these 
fields are not unique, which would raise my curiosity about it and I would research about the source (is 
the feed correct?). The data health check doesn't pass when running the health script, but it doesn't except it
because this is not a critical assumption in order for the transform to be correct.
4. The fields artist_name and track_name don't contain the string "--/". This is due to the fact that we are 
creating a unique identifier for a track by concatenating the two columns. At the end, we split this
concatenation for interpretability by using "--/". If either the artist name or the track name had that substring, the track name
would not be understandable. This check doesn't raise an exception since it doesn't invalidate the results.

## What would I work on if I had more time

I've split this section in 3 parts that I would fine interesting to investigate further. 

### The task itself

**Optimization**: 

In the folder solution you'll find two query plans attached and two .png with a screenshot on
disk spillage for each case. I wanted to reuse the computation right before computing the top sessions
and provided evidence that caching helps on that (see query plans). The intention of the query plan is 
to justify some decisions on how I designed the solution: The windows reuse the same shuffle on user_id
(Exchange hashpartitioning(user_id#47, 200)), caching is reuses computation (numbers in brackets in the cached query plan),
Broadcastjoin to filter on the top_sessions and usage of order + limit (TakeOrderedAndProject) instead of
empty window (less data to shuffle). 

However, there is still some disk spillage. Since it is only a few MB, the transform is quite fast and
I didn't know which machine you'd run this one (that's why I gave freedom on the spark-submit) I decided
not to spent too much time on it. 

**Proper parsing of input dataset**: 

There is one row with two single quotes that is not parsed correctly. 
Would like to read it correctly parsed. The only way this could affect the solution is if this song
was a candidate for the top 10, as it could lose one count. 
Due to limited time and impact (I've checked, it doesn't repeat enough to be a candidate), I left this as a to-do.

### Data engineering

In this section I discuss things I would do if this was part of a data pipeline: 

1. Scalability: Check what happens to the transform if more rows are onboarded (explode on a newly created
array column and replace track_start_timestamp by randomly generated timestamps)
2. track_id: Contact data source to see if we can get a track id for all songs (avoid concatenation workaroound)
3. Normalize tables depending on the uses cases: User, songs, sessions... Why? Could use track_ids and then
join top tracks to songs table to find name etc (less data to shuffle!)
4. Work as this data grows daily and every day we need to compute the all-time top 10 songs. Lots of computation
could be reused from one day to another.
5. Wrap in a class so I have documentation on checks, input/output (data lineage) and business logic. A subclass
for this dataset would be created on top of a Dataset class with methods like write to disk, write a .md with documentation...

### Spark and docker

1. Would have been fun to create a local spark standalone cluster so the spark UI would be always up (instead
of exiting on job completion) and be able to explore how the job is being executed. 
2. Use compose up in docker to create a distributed spark standalone cluster with a few containers. 
3. Handle the logs better than just printing in the console.
4. Create a spillage listener to benchmark optimizations. 

