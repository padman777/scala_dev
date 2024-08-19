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

    print (dneg)



  }
}