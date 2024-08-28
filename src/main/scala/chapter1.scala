object chapter1 {
  def main(args: Array[String]): Unit = {

    //postive and divisible by 2
    val a = 14

    val ac = a > 0 && a % 2 == 0



    // range check with or value less than -10 or greater than 10
    val b = -15

    val bc = b < -10 || b > 10



    // odd and not divisible by 3

    val c =27

    val cc = c %2 == 1 && c % 3 != 0



    // divisibility by 4 and 6

    val d = 18

    val dc = d % 4 == 0 && d % 6 == 0


   // elgibility for voting or driving

    val age = 20

    val dage = age >= 18 || age  >= 16

    // multiple range check (1,10) or (20,30)

    val range = 25

    val drange = (range > 1 && range < 20) || (range > 20 && range < 30)





    ///both negative and odd

    val neg = -7

    val dneg = neg < 0 && neg % 2 !=0



    // student discoutn or senior discount

    var student =63

    var dstudent = student > 60 || student < 25



    // divisiblity by 5 and 7

    var div = 35

    var ddiv = div % 5 ==0 && div % 7 ==0




    // check fr non negative or even

    var abc = -8

    var dabc = abc >0 || abc % 2 ==0



    // check fr prime and odd

    var prime = 17

    //discount or free shipping

    var dis  =120

    var ddis = dis > 150 || dis >100



    // check odd and not divisible by 4

    var t1 =15

    var dt1 =  t1 % 2 !=0 && t1%4 !=0



    // cehck if divisible by 2 or 3

    var t2 = 9

    var dt2 =  t2 % 2 ==0 || t2 % 3 ==0



    // odd or prime
    // unable to find how to write a function for prime numbers


    //





  }
}