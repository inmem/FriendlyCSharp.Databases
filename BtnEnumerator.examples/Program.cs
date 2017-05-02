using FriendlyCSharp.Databases;
using System;
using System.Collections.Generic;

namespace BtnEnumerator.examples
{
  public class TestKV : FcsBTreeN<int, uint>
  {
    protected override bool BtnUpdates(int keyAdd, uint valueAdd, ref uint valueUpdates, object objUpdates)
    {
      valueUpdates++;
      return true;
    }
    //////////////////////////
    protected override int BtnCompares(int keyX, int keyY, object objCmp)
    {
      return keyX - keyY;
    }
    //////////////////////////
    public TestKV() : base(2)
    {
    }
  }

  class Program
  {
    static void Main(string[] args)
    {
      uint uiCount = 1;
      TestKV btnTest = new TestKV();
      // Build the tree
      btnTest.BtnAdd(20, uiCount);
      btnTest.BtnAdd(40, uiCount);
      btnTest.BtnAdd(10, uiCount);
      btnTest.BtnAdd(30, uiCount);
      btnTest.BtnAdd(15, uiCount); //
      btnTest.BtnAdd(35, uiCount);
      btnTest.BtnAdd(7, uiCount);
      btnTest.BtnAdd(26, uiCount);
      btnTest.BtnAdd(18, uiCount);
      btnTest.BtnAdd(22, uiCount); //
      btnTest.BtnAdd(5, uiCount);  //
      btnTest.BtnAdd(42, uiCount);
      btnTest.BtnAdd(13, uiCount);
      btnTest.BtnAdd(46, uiCount);
      btnTest.BtnAdd(27, uiCount);
      btnTest.BtnAdd(27, uiCount); // duplicity call BtnUpdates()
      btnTest.BtnAdd(8, uiCount);
      btnTest.BtnAdd(32, uiCount); //
      btnTest.BtnAdd(38, uiCount);
      btnTest.BtnAdd(24, uiCount);
      btnTest.BtnAdd(45, uiCount);
      btnTest.BtnAdd(25, uiCount); //

      // output: 5,7,8,10,13,15,18,20,22,24,25,26,27,30,32,35,38,40,42,45,46,
      foreach(KeyValuePair<int, uint>? keyValue in btnTest)
        Console.Write(keyValue.GetValueOrDefault().Key + ",");
      Console.WriteLine();

      // output: 5,7,8,10,13,15,18,20,22,24,25,26,
      FcsBTreeN<int, uint>.BtnEnumerator btnE = FcsBTreeN<int, uint>.GetEnumeratorEx(btnTest, false, 12);
      while (btnE.MoveNext())
        Console.Write(btnE.Current.GetValueOrDefault().Key + ",");
      btnE.Dispose();
      Console.WriteLine();

      int? keyLo = 22;
      int? keyHi = 38;
      // output: 5,7,8,10,13,15,18,20,22,24,25,26,27,30,32,35,38,
      btnE = FcsBTreeN<int, uint>.GetEnumeratorEx(btnTest, null, keyHi, false);
      while (btnE.MoveNext())
        Console.Write(btnE.Current.GetValueOrDefault().Key + ",");
      btnE.Dispose();
      Console.WriteLine();

      // output: 22,24,25,26,27,30,32,35,38,40,42,45,46,
      btnE = FcsBTreeN<int, uint>.GetEnumeratorEx(btnTest, keyLo, null, false);
      while (btnE.MoveNext())
        Console.Write(btnE.Current.GetValueOrDefault().Key + ",");
      btnE.Dispose();
      Console.WriteLine();

      // output: 22,24,25,26,27,30,32,35,38,
      btnE = FcsBTreeN<int, uint>.GetEnumeratorEx(btnTest, keyLo, keyHi, false);
      while (btnE.MoveNext())
        Console.Write(btnE.Current.GetValueOrDefault().Key + ",");
      btnE.Dispose();
      Console.WriteLine();

      // output: 22,24,25,26,27,
      btnE = FcsBTreeN<int, uint>.GetEnumeratorEx(btnTest, keyLo, keyHi, false, 5);
      while (btnE.MoveNext())
        Console.Write(btnE.Current.GetValueOrDefault().Key + ",");
      btnE.Dispose();
      Console.WriteLine();

      // ---------- REVERSE ---------- 
      // output: 46,45,42,40,38,35,32,30,27,26,25,24,22,20,18,15,13,10,8,7,5,
      btnE = FcsBTreeN<int, uint>.GetEnumeratorEx(btnTest, true);
      while (btnE.MoveNext())
        Console.Write(btnE.Current.GetValueOrDefault().Key + ",");
      btnE.Dispose();
      Console.WriteLine();

      // output: 46,45,42,40,38,35,32,30,27,26,25,24,
      btnE = FcsBTreeN<int, uint>.GetEnumeratorEx(btnTest, true, 12);
      while (btnE.MoveNext())
        Console.Write(btnE.Current.GetValueOrDefault().Key + ",");
      btnE.Dispose();
      Console.WriteLine();

      // output: 38,35,32,30,27,26,25,24,22,20,18,15,13,10,8,7,5,
      btnE = FcsBTreeN<int, uint>.GetEnumeratorEx(btnTest, null, keyHi, true);
      while (btnE.MoveNext())
        Console.Write(btnE.Current.GetValueOrDefault().Key + ",");
      btnE.Dispose();
      Console.WriteLine();

      // output: 46,45,42,40,38,35,32,30,27,26,25,24,22,
      btnE = FcsBTreeN<int, uint>.GetEnumeratorEx(btnTest, keyLo, null, true);
      while (btnE.MoveNext())
        Console.Write(btnE.Current.GetValueOrDefault().Key + ",");
      btnE.Dispose();
      Console.WriteLine();

      // output: 38,35,32,30,27,26,25,24,22,
      btnE = FcsBTreeN<int, uint>.GetEnumeratorEx(btnTest, keyLo, keyHi, true);
      while (btnE.MoveNext())
        Console.Write(btnE.Current.GetValueOrDefault().Key + ",");
      btnE.Dispose();
      Console.WriteLine();

      // output: 38,35,32,30,27,
      btnE = FcsBTreeN<int, uint>.GetEnumeratorEx(btnTest, keyLo, keyHi, true, 5);
      while (btnE.MoveNext())
        Console.Write(btnE.Current.GetValueOrDefault().Key + ",");
      btnE.Dispose();
      Console.WriteLine();

      Console.WriteLine();
      Console.WriteLine("Key ENTER press.");
      Console.ReadLine();
    }
  }
}