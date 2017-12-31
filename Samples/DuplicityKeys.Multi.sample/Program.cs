using FriendlyCSharp.Databases;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.InteropServices;

namespace DuplicityKeys.Multi.sample
{
  class Program
  {
    //
    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    private struct BtnKey
    {
      public string keyCity;
      public long keyId;
      //public DateTime key0;
      //public string key1;
      //public long key2;
      public int valueCount;
      public DateTime[] aValueDateTime;
      public uint[] aValueRand;
      //
      static int CmpBtnKey(BtnKey keyX, BtnKey keyY, object objCmp, object objCmp2)
      {
        // return < 0 (less), = 0 (equal), > 0 (greater)
        int iResult = String.Compare(keyX.keyCity, keyY.keyCity, StringComparison.Ordinal);
        if (iResult == 0)
        {
          iResult = keyX.keyId.CompareTo(keyY.keyId);
          //if (iResult == 0)
          //{
          //  iResult = DateTime.Compare(key0, keyY.key0);
          //  if (iResult == 0)
          //  {
          //    iResult = string.Compare(key1, keyY.key1, true);
          //    if (iResult == 0)
          //    {
          //      iResult = key2.CompareTo(keyY.key2);
          //    }
          //  }
          //}
        }
        return iResult;
      }
      //
      static bool FuncUpdate(BtnKey keyAdd, ref BtnKey keyUpdate, object objUpdate)
      {
        // Resize ?
        if ((keyUpdate.aValueDateTime.Length <= keyUpdate.valueCount) || (keyUpdate.aValueRand.Length <= keyUpdate.valueCount))
        {
          if ((keyUpdate.aValueDateTime.Length >= 1024) || (keyUpdate.aValueRand.Length >= 1024))
          {
            Array.Resize<DateTime>(ref keyUpdate.aValueDateTime, keyUpdate.aValueDateTime.Length + 1024);
            Array.Resize<uint>(ref keyUpdate.aValueRand, keyUpdate.aValueRand.Length + 1024);
          }
          else
          {
            Array.Resize<DateTime>(ref keyUpdate.aValueDateTime, keyUpdate.aValueDateTime.Length * 2);
            Array.Resize<uint>(ref keyUpdate.aValueRand, keyUpdate.aValueRand.Length * 2);
          }
        }
        // Update
        keyUpdate.aValueDateTime[keyUpdate.valueCount] = keyAdd.aValueDateTime[0];
        keyUpdate.aValueRand[keyUpdate.valueCount] = keyAdd.aValueRand[0];
        keyUpdate.valueCount++;
        return true;
      }
      //
      public static FcsKeyFastBTreeN<BtnKey> CreateFcsKeyFastBTreeN()
      {
        return new FcsKeyFastBTreeN<BtnKey>(CmpBtnKey, FuncUpdate, 32);
      }
    }
    //
    //
    //
    static void Main(string[] args)
    {
      Console.OutputEncoding = System.Text.Encoding.UTF8;
      Console.WriteLine(String.Format("DuplicityKeys.Milti.sample, {0}", (IntPtr.Size == 4) ? "32 bit" : "64 bit"));
      Console.WriteLine("-----------------------------------");

      int max = 36000;
      string[] aCity = { "London", "Moscow", "Warsaw", "Berlin", "Paris", "Prague", "Brussels", "Vienna", "Zagreb", "Helsinki" };
      int iPocetAdd = 0;
      Random r = new Random((int)DateTime.Now.Ticks);
      uint[] aRand = new uint[max];
      var hashSetRand = new HashSet<uint>();
      while (iPocetAdd < max)
      {
        uint rand = (uint)r.Next(0, Int32.MaxValue - 1);
        if (hashSetRand.Contains(rand) == false)
        {
          hashSetRand.Add(rand);
          aRand[iPocetAdd] = rand;
          iPocetAdd++;
        }
      }
      hashSetRand = null;
      long iMem = GC.GetTotalMemory(true);
      Console.WriteLine("UsedMemory {0,4} MB", iMem >> 20);
      long iMemOld = iMem;
      //
      //
      iPocetAdd = 0;
      Console.WriteLine("-----------------------------------");
      Console.WriteLine("FcsFastBTreeN");
      BtnKey key;
      key.aValueRand = new uint[1];
      key.aValueDateTime = new DateTime[1];
      FcsKeyFastBTreeN<BtnKey> btnTest = BtnKey.CreateFcsKeyFastBTreeN();
      var swFcsKV = Stopwatch.StartNew();
      for (int iter = 0; iter < 100; iter++)
      {
        for (int idx = 0; idx < max; idx++)
        {
          key.keyCity = aCity[idx % aCity.Length];
          key.keyId = idx % 1200;
          key.valueCount = 1;
          key.aValueRand[0] = aRand[idx];
          key.aValueDateTime[0] = DateTime.Now;
          if (btnTest.BtnAdd(key) != null)
            iPocetAdd++;
        }
      }
      swFcsKV.Stop();
      iMem = GC.GetTotalMemory(true);
      Console.WriteLine("UsedMemory {0,5} MB [{1,5:N1} s] | {3} keys | Δ {2,3} MB | {4,8:N0} ns   | {5,10:N0} values | sizeT {6,2} Byte", iMem >> 20, swFcsKV.Elapsed.TotalSeconds, (iMem - iMemOld) >> 20,
                        btnTest.BtnUsedKeys(), ((double)(swFcsKV.Elapsed.TotalMilliseconds * 1000000) / iPocetAdd), iPocetAdd, Marshal.SizeOf<BtnKey>());
      iMemOld = iMem;
      int iCompareCount = 0;
      // generate code at run time 
      foreach (BtnKey? value in btnTest)
        iCompareCount++;
      // 
      iCompareCount = 0;
      swFcsKV.Restart();
      foreach (BtnKey? value in btnTest)
        iCompareCount++;
      swFcsKV.Stop();
      Console.WriteLine("\nFcsKeyFastBTreeN - foreach()");
      Console.WriteLine($"{((double)(swFcsKV.Elapsed.TotalMilliseconds * 1000000) / iCompareCount),9:N2} ns  [{swFcsKV.Elapsed.TotalMilliseconds,11} ms | {iCompareCount} keys ]{iCompareCount / swFcsKV.Elapsed.TotalSeconds,20:N0} IOPS");

      iCompareCount = 0;
      FcsKeyFastBTreeN<BtnKey>.KeyEnumerator btnEn = btnTest.GetEnumeratorFastEx(false);
      swFcsKV.Restart();
      while (btnEn.MoveNext())
      {
        BtnKey? value = btnEn.Current;
        iCompareCount++;
      }
      swFcsKV.Stop();
      btnEn.Dispose();
      Console.WriteLine("\nFcsKeyFastBTreeN - foreach()");
      Console.WriteLine($"{((double)(swFcsKV.Elapsed.TotalMilliseconds * 1000000) / iCompareCount),9:N2} ns  [{swFcsKV.Elapsed.TotalMilliseconds,11} ms | {iCompareCount} keys ]{iCompareCount / swFcsKV.Elapsed.TotalSeconds,20:N0} IOPS");

      iCompareCount = 0;
      FcsKeyFastBTreeN<BtnKey>.KeyFast btnFast;
      swFcsKV.Restart();
      if (btnTest.BtnFastFirst(out BtnKey fcsKey2, out btnFast) != null)
      {
        do
        {
          iCompareCount++;
        }
        while (btnTest.BtnFastNext(ref fcsKey2, ref btnFast) != null);
      }
      swFcsKV.Stop();
      btnFast.Dispose();
      Console.WriteLine("\nFcsFastBTreeN - BtnFastFirst()/BtnFastNext()");
      Console.WriteLine($"{((double)(swFcsKV.Elapsed.TotalMilliseconds * 1000000) / iCompareCount),9:N2} ns  [{swFcsKV.Elapsed.TotalMilliseconds,11} ms | {iCompareCount} keys ]{iCompareCount / swFcsKV.Elapsed.TotalSeconds,20:N0} IOPS");
      //
      //
      Console.WriteLine("-----------------------------------");
      Console.WriteLine("Key ENTER press.");
      Console.ReadLine();
    }
  }
}