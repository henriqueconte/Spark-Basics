// Question 1: 1. charger le fichier de tweets fourni dans un RDD en utilisation la fonction sc.textFile
val tweets = sc.textFile("/user/4/figueirh/Desktop/ENSIMAG/GestionDonnes/ScalaSpark/1Mtweets_en.txt")

// Question 2:creer un RDD contenant tous les tweets mentionnant Donald Trump. Affichez le nombre d'elements de ce RDD.
val trumpTweets = tweets.filter(tweet => tweet.toLowerCase().contains("trump"))

// Question 3: en utilisant la librairire TweeetUtilities3 construisez un RDD qui contient des cou- ples (tweet, sentiments). Affichez les 5 premiers éléments de ce RDD en utilisant take(5).foreach(println).
:load TweetUtilities.scala
val sentimentTweets = tweets.map(tweet => (tweet, TweetUtilities.getSentiment(tweet)))

// Question 4: en utilisant ce RDD, créer un RDD qui représente le sentiment associé à chaque Hashtag. Affichez les 5 premiers éléments de ce RDD en utilisant take(5).foreach(println).
val hashtagsRdd = tweets.flatMap(tweet => TweetUtilities.getHashTags(tweet)).distinct
val sentimentHashtags = hashtagsRdd.map(hashtag => (hashtag, TweetUtilities.getSentiment(hashtag)))

// Question 5: Vous pouvez maintenant afficher le top K des hashtags postifs et négatifs en utilisant top(k) et takeOrdered(k).
val bestHashtags = sentimentHashtags.top(10)(Ordering[Double].on(_._2))
val worstHashtags = sentimentHashtags.takeOrdered(10)(Ordering[Double].on(_._2))

