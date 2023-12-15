/*
Student Name: Patrick O'Connor
Student ID: 20040412
gitHub repo: https://github.com/chipspeak/database_assignment2
*/

//Part 1 ---Queries----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


//1. Horror films from 1980 to present that are less than an R rating and have an imdb rating of 7.0 or above.
db.movies
  .find(
    {
      genres: "Horror",
      rated: { $nin: ["R", "NC-17"] },
      "imdb.rating": { $gte: 7 },
      year: { $gte: 1980 },
    },
    {
      _id: 0,
      title: 1,
      year: 1,
      directors: 1,
      cast: 1,
      genres: 1,
      plot: 1,
      rated: 1,
      "imdb.rating": 1,
    }
  )
  .sort({ "imdb.rating": -1 })
  .limit(10)
  .pretty();


//2. Romantic comedies starring Adam Sandler with an rotten tomatoes viewer rating of 3 or above.
db.movies
  .find(
    {
      genres: { $all: ["Comedy", "Romance"] },
      cast: { $in: ["Adam Sandler"] },
      "tomatoes.viewer.rating": { $gte: 3 },
    },
    {
      _id: 0,
      title: 1,
      year: 1,
      cast: 1,
      genres: 1,
      plot: 1,
      "tomatoes.viewer.rating": 1,
    }
  )
  .sort({ "tomatoes.viewer.rating": -1 })
  .limit(20)
  .pretty();


//3. Multi-Award winning documentaries with a run time of less than 2 hours (regex to match the word wins for awards).
db.movies
  .find(
    {
      genres: "Documentary",
      "imdb.rating": { $gte: 9 },
      awards: /wins/,
      runtime: { $lte: 120 },
    },
    {
      _id: 0,
      title: 1,
      year: 1,
      cast: 1,
      genres: 1,
      awards: 1,
      runtime: 1,
      "imdb.rating": 1,
    }
  )
  .sort({ "imdb.rating": -1 })
  .limit(10)
  .pretty();


//4. A count of films directed by Steven Spielberg and starring Tom Hanks from 2010 onwards
db.movies
  .find({
    $and: [
      { directors: "Steven Spielberg" },
      { cast: { $elemMatch: { $eq: "Tom Hanks" } } },
      { year: { $gte: 2010 } },
    ],
  })
  .count();


//5. Highest rated items on IMDB from the 1990s excluding documentaries
db.movies
  .find(
    {
      $and: [
        { "imdb.rating": { $gte: 8 } },
        { year: { $gte: 1990, $lte: 1999 } },
        { genres: { $nin: ["Documentary"] } },
      ],
    },
    { _id: 0, title: 1, year: 1, cast: 1, plot: 1, "imdb.rating": 1, genres: 1 }
  )
  .sort({ "imdb.rating": -1 })
  .limit(5)
  .pretty();


//6. A count of the amount of War films based upon a novel that appear in the database that were made prior to 1966 with a runtime of over 2 hours
db.movies
  .find({
    writers: { $elemMatch: { $regex: /novel/i } },
    genres: "War",
    year: { $lt: 1966 },
    runtime: { $gt: 120 },
  })
  .count();



//Part 2 ---Inserts--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

//(films)
//insertMany is used to insert Dune(2021) and Joker(2019)
db.movies.insertMany([
  {
    _id: 611991,
    title: "Dune",
    year: 2021,
    runtime: 155,
    directors: ["Denis Villeneuve"],
    cast: ["Timothée Chalamet", "Rebecca Ferguson", "Zendaya"],
    plot: "Set in the far distant future, in a post-artificial intelligence universe, the film follows Paul Atreides as his family, the noble House Atreides, is thrust into a war for the deadly and inhospitable desert planet Arrakis",
    genres: ["Sci-Fi", "Drama"],
    imdb: { rating: 8.0, votes: 732664 },
  },
  {
    _id: 611992,
    title: "Joker",
    year: 2019,
    runtime: 122,
    directors: ["Todd Phillips"],
    cast: ["Joaquin Phoenix", "Robert De Niro", "Zazie Beetz"],
    plot: "Set in 1981, it follows Arthur Fleck, a failed clown and aspiring stand-up comic whose descent into mental illness and nihilism inspires a violent countercultural revolution against the wealthy in a decaying Gotham City.",
    genres: ["Thriller", "Psychological"],
    imdb: { rating: 8.4, votes: 1400000 },
  },
]);
//find to check for Dune after insertion
db.movies.find({ _id: 611991 });
//find to check for Joker after insertion
db.movies.find({ _id: 611992 });


//(users)
//insertMany is used to insert 3 user documents with an array of favourites for each user alongside typical properties like email and password
db.users.insertMany([
  {
    _id: 611993,
    name: "Pat O Connor",
    email: "pat@email.com",
    password: "itsAsecret",
    favourites: [
      ObjectId("573a13bff29313caabd5f799"),
      ObjectId("573a13cdf29313caabd844c7"),
    ],
  },
  {
    _id: 611994,
    name: "Homer Simpson",
    email: "homerS@springfieldPowerPlant.org",
    password: "password",
    favourites: [
      ObjectId("573a1396f29313caabce3c3e"),
      ObjectId("573a1399f29313caabcec9e8"),
    ],
  },
  {
    _id: 611995,
    name: "Dougal MacGuire",
    email: "inept@priests.ie",
    password: "1234",
    favourites: [
      ObjectId("573a1398f29313caabce9d81"),
      ObjectId("573a1398f29313caabcea86a"),
    ],
  },
]);
//check for user upon creation
db.users.find({ _id: 611993 });



//Part 3 ---Update,Delete------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
//(updates)

//updates Dune's imdb score to 8.1 and increases its imdb votes by 127
db.movies.updateOne(
  { _id: 611991 },
  { $set: { "imdb.rating": 8.5 }, $inc: { "imdb.votes": 1 } }
);
//find to check for Dune after update
db.movies.find({ _id: 611991 });


//updates Pat's favourites with Dune as an entry
db.users.updateOne({ _id: 611993 }, { $push: { favourites: 611991 } });
//find to check push was successful
db.users.find({ _id: 611993 });


//updates Joker with an awards field
db.movies.updateOne(
  { _id: 611992 },
  {
    $set: {
      awards:
        "Won 2 Oscars. Best Actor for Jaoquin Phoenix and Best Original Score for Hildur Gudnadottir",
    },
  }
);
//find to check addition of awards field was successful
db.movies.find({ _id: 611992 });


//updates Dune with a writers field
db.movies.updateOne(
  { _id: 611991 },
  {
    $set: {
      writers: [
        "Frank Herbert(Novel)",
        "Denis Villeneuve",
        "Jon Spaihts",
        "Eric Roth",
      ],
    },
  }
);
//find to check for Dune after update
db.movies.find({ _id: 611991 });


//(delete)
//deleting Dune
db.movies.deleteOne({ _id: 611991 });
//find to check for Dune after delete
db.movies.find({ _id: 611991 });



//Part 4 ---Aggregate pipelines----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

//1. --- Films where the director also stars sorted by number of films in which they appear and direct. Also features the range of years in which these films occur.
/*
$match for non-null directors and cast. $unwind is used to create a new document from the directors array for each individual element.
$addFields is used to create a new field directorInCast which will be set to true if the director also appears in the cast array in addition an average of imdb ratings
$match is used again to check for when directorInCast is true meaning the director appears in the films cast
$group is then used to group the documents by directors and totalFilms is calculated using $sum on the documents of each group
$min and $max are also used on the year fields for use in displaying the earliest and latest film from the list
$project is used to format the display to id, totalfilms, earliest and latest before sorting by totalFilms in which they direct and appear
*/
db.movies
  .aggregate([
    {
      $match: { $and: [{ directors: { $ne: null } }, { cast: { $ne: null } }] },
    },
    { $unwind: "$directors" },
    { $addFields: { directorInCast: { $in: ["$directors", "$cast"] } } },
    { $match: { directorInCast: true } },
    {
      $group: {
        _id: "$directors",
        totalFilms: { $sum: 1 },
        earliest: { $min: "$year" },
        latest: { $max: "$year" },
      },
    },
    { $project: { _id: 1, totalFilms: 1, earliest: 1, latest: 1 } },
    { $sort: { totalFilms: -1 } },
    { $limit: 10 },
  ])
  .pretty();

//2. --- Genres that Cersei Lannister comments on most and their average Rotten Tomatoes Rating
/*
$match is used to match the name field with Cersei Lannister. The comments document is unwound creating a new document for each comment.
$match is used again on the unwound document to match the name before genres is unwound creating a document for each genre on which the comments appear.
Worth noting that this may slightly alter finds as it's counting comments on multi-genred films as separate i.e "War, Romance" will both receive individual entries
Thus counting as two separate genres receiving comment rather than joined. This is purely for the tidiness of aesthetics.
$group is then used to group documents by genre before $sum is used to count the number of documents (and therefore the amount of comments i.e commentCount)
Additionally averageTomatoesRating is declared as the $avg of the tomatoes ratings on the documents
$project is then used to format the output. As opposed to the above aggregate, in this one id is left off the output and $round is used limit the average to 1 decimal place.
*/
db.movies.aggregate([
  { $match: { "comments.name": "Cersei Lannister" } },
  { $unwind: "$comments" },
  { $match: { "comments.name": "Cersei Lannister" } },
  { $unwind: "$genres" },
  {
    $group: {
      _id: "$genres",
      commentCount: { $sum: 1 },
      averageTomatoesRating: { $avg: "$tomatoes.viewer.rating" },
    },
  },
  {
    $project: {
      _id: 0,
      genre: "$_id",
      commentCount: 1,
      averageTomatoesRating: { $round: ["$averageTomatoesRating", 1] },
    },
  },
  { $sort: { commentCount: -1 } },
  { $limit: 5 },
]);
