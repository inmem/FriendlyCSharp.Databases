using System;
using System.Collections.Generic;
using FriendlyCSharp.Databases;

namespace BtnEnumerator.NET.examples
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
      // return less < 0, equal = 0, greater > 0
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
      Console.OutputEncoding = System.Text.Encoding.Unicode;
      Console.WriteLine("BtnEnumerator.NET452.examples");
      Console.WriteLine("-----------------------------");

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
      btnTest.BtnAdd(27, uiCount); // duplicity call BtnUpdates()
      btnTest.BtnAdd(45, uiCount);
      btnTest.BtnAdd(25, uiCount); //

      // output: 5,7,8,10,13,15,18,20,22,24,25,26,27,30,32,35,38,40,42,45,46,
      foreach (KeyValuePair<int, uint>? keyValue in btnTest)
        Console.Write(keyValue.GetValueOrDefault().Key + ",");
      Console.WriteLine();

      // output: 5,7,8,10,13,15,18,20,22,24,25,26,
      FcsBTreeN<int, uint>.BtnEnumerator btnEn = btnTest.GetEnumeratorEx(false, 12);
      while (btnEn.MoveNext())
        Console.Write(btnEn.Current.GetValueOrDefault().Key + ",");
      btnEn.Dispose();
      Console.WriteLine();

      int? keyLo = 22;
      int? keyHi = 38;
      // output: 5,7,8,10,13,15,18,20,22,24,25,26,27,30,32,35,38,
      btnEn = btnTest.GetEnumeratorEx(null, keyHi, false);
      while (btnEn.MoveNext())
        Console.Write(btnEn.Current.GetValueOrDefault().Key + ",");
      Console.WriteLine();
      btnEn.Dispose();

      // output: 22,24,25,26,27,30,32,35,38,40,42,45,46,
      btnEn = btnTest.GetEnumeratorEx(keyLo, null, false);
      while (btnEn.MoveNext())
        Console.Write(btnEn.Current.GetValueOrDefault().Key + ",");
      Console.WriteLine();
      btnEn.Dispose();

      // output: 22,24,25,26,27,30,32,35,38,
      btnEn = btnTest.GetEnumeratorEx(keyLo, keyHi, false);
      while (btnEn.MoveNext())
        Console.Write(btnEn.Current.GetValueOrDefault().Key + ",");
      Console.WriteLine();
      // output: 22,24,25,26,27,30,32,35,38,
      btnEn.Reset();
      while (btnEn.MoveNext())
        Console.Write(btnEn.Current.GetValueOrDefault().Key + ",");
      Console.WriteLine();
      btnEn.Dispose();

      // output: 22,24,25,26,27,
      btnEn = btnTest.GetEnumeratorEx(keyLo, keyHi, false, 5);
      while (btnEn.MoveNext())
        Console.Write(btnEn.Current.GetValueOrDefault().Key + ",");
      Console.WriteLine();
      btnEn.Dispose();

      // ---------- REVERSE ---------- 
      Console.WriteLine("-----------------------------");
      // output: 46,45,42,40,38,35,32,30,27,26,25,24,22,20,18,15,13,10,8,7,5,
      btnEn = btnTest.GetEnumeratorEx(true);
      while (btnEn.MoveNext())
        Console.Write(btnEn.Current.GetValueOrDefault().Key + ",");
      Console.WriteLine();
      btnEn.Dispose();

      // output: 46,45,42,40,38,35,32,30,27,26,25,24,
      btnEn = btnTest.GetEnumeratorEx(true, 12);
      while (btnEn.MoveNext())
        Console.Write(btnEn.Current.GetValueOrDefault().Key + ",");
      Console.WriteLine();
      btnEn.Dispose();

      // output: 38,35,32,30,27,26,25,24,22,20,18,15,13,10,8,7,5,
      btnEn = btnTest.GetEnumeratorEx(null, keyHi, true);
      while (btnEn.MoveNext())
        Console.Write(btnEn.Current.GetValueOrDefault().Key + ",");
      Console.WriteLine();
      btnEn.Dispose();

      // output: 46,45,42,40,38,35,32,30,27,26,25,24,22,
      btnEn = btnTest.GetEnumeratorEx(keyLo, null, true);
      while (btnEn.MoveNext())
        Console.Write(btnEn.Current.GetValueOrDefault().Key + ",");
      Console.WriteLine();
      btnEn.Dispose();

      // output: 38,35,32,30,27,26,25,24,22,
      btnEn = btnTest.GetEnumeratorEx(keyLo, keyHi, true);
      while (btnEn.MoveNext())
        Console.Write(btnEn.Current.GetValueOrDefault().Key + ",");
      Console.WriteLine();
      btnEn.Dispose();

      // output: 38,35,32,30,27,
      btnEn = btnTest.GetEnumeratorEx(keyLo, keyHi, true, 5);
      while (btnEn.MoveNext())
        Console.Write(btnEn.Current.GetValueOrDefault().Key + ",");
      Console.WriteLine();
      btnEn.Dispose();

      Console.WriteLine("-----------------------------");
      Console.WriteLine("Key ENTER press.");
      Console.ReadLine();
    }
  }
}
