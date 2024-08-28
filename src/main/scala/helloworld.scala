object helloworld {
  def main(args:Array[String]):Unit ={

    //palindrome and reversal numbers


//   var num =444
//    var original=num
//    var reverse =0
//    var rem =0
//
//    while (num!=0){
//      rem=num%10
//      reverse =reverse*10+rem
//      num=num/10
//
//
//      }
//      if (reverse==original){
//        print ("palindrome")
//
//
//      }
//      else {
//        print ("not palindrome")
//
//      }

    //fibonacci series

//    var a = 0
//    var b =a+1
//    var c =0
//    var count =0
//    while (count<10) {
//      var c =a+b
//      a=b
//      b=c
//      print (c+ " ")
//      count =count+1
//    }


    //dynamic array


    println("enter the size")
    val size=scala.io.StdIn.readInt()
    val arr=new Array[Int](size)
    println("enter the elements ")

    for(i<-0 until arr.length){

      println("enter the elements at"+" "+i)
      arr(i)=scala.io.StdIn.readInt()

    }
    println("display the elements ")
    for(i<-0 until arr.length) {

      println(arr(i))

    }


    }







}


