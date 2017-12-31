using System;
using System.Collections.Generic;
using System.Diagnostics;
using FriendlyCSharp.Databases;

namespace FcsFastBTreeN.Multi.benchmark
{
  class Program
  {
    private class TestKV : FcsFastBTreeN<int, uint>
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
      public TestKV() : base(32, 32)
      {
      }
    }
    //
    private struct StructBtn
    {
      public int key;
      public uint value;
      public int CompareTo(StructBtn keyY)
      {
        return key - keyY.key;
      }
    }
    // EqualityComparer<StructBtn>  
    private class StructBtnComparer : EqualityComparer<StructBtn>
    {
      public override bool Equals(StructBtn x, StructBtn y)
      {
        return (x.key.CompareTo(y.key) == 0);
      }
      public override int GetHashCode(StructBtn x)
      {
        return x.key.GetHashCode();
      }
    }
    //
    //
    //
    const int _max = 1;
    static void Main(string[] args)
    {
      Console.OutputEncoding = System.Text.Encoding.UTF8;
      Console.WriteLine(String.Format("FcsFastBTreeN.Multi.benchmark, {0}", (IntPtr.Size == 4) ? "32 bit" : "64 bit"));
      Console.WriteLine("--------------------------------------");

      int iPocetAdd = 0;
      Random r = new Random((int)DateTime.Now.Ticks);
      int[] aRand = new int[10000000];
      var hashSetRand = new HashSet<int>();
      while (iPocetAdd < 10000000)
      {
        int rand = r.Next(1, Int32.MaxValue);
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
      Console.WriteLine("--------------------------------------");
      Console.WriteLine("FcsFastBTreeN");
      TestKV btnTest = new TestKV();
      var swFcsKV = Stopwatch.StartNew();
      for (int i = 0; i < _max; i++)
      {
        uint value = 1;
        foreach (int ii in aRand)
          btnTest.BtnAdd(ii, ref value, null);
      }
      swFcsKV.Stop();
      iMem = GC.GetTotalMemory(true);
      Console.WriteLine("UsedMemory {0,4} MB [{1,4:N1} s] | {3} | Δ {2,3} MB | {4,8:N0} ns", iMem >> 20, swFcsKV.Elapsed.TotalSeconds, (iMem - iMemOld) >> 20,
                        btnTest.BtnUsedKeys(), ((double)(swFcsKV.Elapsed.TotalMilliseconds * 1000000) / _max / aRand.Length));
      iMemOld = iMem;
      int iCompareCount = 0;
      swFcsKV.Restart();
      foreach (KeyValuePair<int, uint>? value in btnTest)
        iCompareCount++;
      swFcsKV.Stop();
      Console.WriteLine("FcsFastBTreeN - foreach()");
      Console.WriteLine($"{((double)(swFcsKV.Elapsed.TotalMilliseconds * 1000000) / iCompareCount),7:N2} ns  [{swFcsKV.Elapsed.TotalMilliseconds,11} ms | {iCompareCount} ]{iCompareCount / swFcsKV.Elapsed.TotalSeconds,20:N0} IOPS");

      iCompareCount = 0;
      swFcsKV.Restart();
      if (btnTest.BtnFastFirst(out int fcsKey2, out uint fcsValue2, 2) != null)
      {
        iCompareCount++;
        while (btnTest.BtnFastNext(ref fcsKey2, out fcsValue2, 2) != null)
          iCompareCount++;
      }
      swFcsKV.Stop();
      Console.WriteLine("FcsFastBTreeN - BtnFastFirst()/BtnFastNext()");
      Console.WriteLine($"{((double)(swFcsKV.Elapsed.TotalMilliseconds * 1000000) / iCompareCount),7:N2} ns  [{swFcsKV.Elapsed.TotalMilliseconds,11} ms | {iCompareCount} ]{iCompareCount / swFcsKV.Elapsed.TotalSeconds,20:N0} IOPS");
      //
      //
      Console.WriteLine("--------------------------------------");
      Console.WriteLine("SortedSet");
      SortedSet<StructBtn> sorted = new SortedSet<StructBtn>(Comparer<StructBtn>.Create((a, b) => a.CompareTo(b)));
      var swSorted = Stopwatch.StartNew();
      for (int i = 0; i < _max; i++)
      {
        StructBtn kvSet;
        kvSet.value = 1;
        foreach (int ii in aRand)
        {
          kvSet.key = ii;
          sorted.Add(kvSet);
        }
      }
      swSorted.Stop();
      iMem = GC.GetTotalMemory(true);
      Console.WriteLine("UsedMemory {0,4} MB [{1,4:N1} s] | {3} | Δ {2,3} MB | {4,8:N0} ns", iMem >> 20, swSorted.Elapsed.TotalSeconds, (iMem - iMemOld) >> 20,
                        sorted.Count, ((double)(swSorted.Elapsed.TotalMilliseconds * 1000000) / _max / aRand.Length));
      iMemOld = iMem;
      iCompareCount = 0;
      swSorted.Restart();
      foreach (StructBtn sortedSetX in sorted)
        iCompareCount++;
      swSorted.Stop();
      Console.WriteLine("SortedSet - foreach()");
      Console.WriteLine($"{((double)(swSorted.Elapsed.TotalMilliseconds * 1000000) / iCompareCount),7:N2} ns  [{swSorted.Elapsed.TotalMilliseconds,11} ms | {iCompareCount} ]{iCompareCount / swSorted.Elapsed.TotalSeconds,20:N0} IOPS");
      //
      //
      Console.WriteLine("--------------------------------------");
      Console.WriteLine("HashSet");
      var hash = new HashSet<StructBtn>(new StructBtnComparer());
      var swHash = Stopwatch.StartNew();
      for (int i = 0; i < _max; i++)
      {
        StructBtn kvSet;
        kvSet.value = 1;
        foreach (int ii in aRand)
        {
          kvSet.key = ii;
          hash.Add(kvSet);
        }
      }
      swHash.Stop();
      iMem = GC.GetTotalMemory(true);
      Console.WriteLine("UsedMemory {0,4} MB [{1,4:N1} s] | {3} | Δ {2,3} MB | {4,8:N0} ns", iMem >> 20, swHash.Elapsed.TotalSeconds, (iMem - iMemOld) >> 20,
                        hash.Count, ((double)(swHash.Elapsed.TotalMilliseconds * 1000000) / _max / aRand.Length));
      iMemOld = iMem;
      iCompareCount = 0;
      swHash.Restart();
      foreach (StructBtn sortedSetH in hash)
        iCompareCount++;
      swSorted.Stop();
      Console.WriteLine("HashSet - foreach()");
      Console.WriteLine($"{((double)(swHash.Elapsed.TotalMilliseconds * 1000000) / iCompareCount),7:N2} ns  [{swSorted.Elapsed.TotalMilliseconds,11} ms | {iCompareCount} ]{iCompareCount / swHash.Elapsed.TotalSeconds,20:N0} IOPS");
      //
      //
      Console.WriteLine("--------------------------------------");
      Console.WriteLine("Dictionary");
      var dic = new Dictionary<int, uint>();
      var swDic = Stopwatch.StartNew();
      for (int i = 0; i < _max; i++)
      {
        foreach (int ii in aRand)
          dic.Add(ii, 1);
      }
      swDic.Stop();
      iMem = GC.GetTotalMemory(true);
      Console.WriteLine("UsedMemory {0,4} MB [{1,4:N1} s] | {3} | Δ {2,3} MB | {4,8:N0} ns", iMem >> 20, swDic.Elapsed.TotalSeconds, (iMem - iMemOld) >> 20,
                        dic.Count, ((double)(swDic.Elapsed.TotalMilliseconds * 1000000) / _max / aRand.Length));
      iMemOld = iMem;
      iCompareCount = 0;
      swDic.Restart();
      foreach (KeyValuePair<int, uint> sortedSetD in dic)
        iCompareCount++;
      swSorted.Stop();
      Console.WriteLine("Dictionary - foreach()");
      Console.WriteLine($"{((double)(swDic.Elapsed.TotalMilliseconds * 1000000) / iCompareCount),7:N2} ns  [{swSorted.Elapsed.TotalMilliseconds,11} ms | {iCompareCount} ]{iCompareCount / swDic.Elapsed.TotalSeconds,20:N0} IOPS");
      //
      //
      iCompareCount = 0;
      int iCompareEquals = 0;
      Console.WriteLine("--------------------------------------");
      Console.WriteLine("Checking for sorting and matching");
      btnTest.BtnFastFirst(out fcsKey2, out fcsValue2, 2);
      foreach (StructBtn sortedSet in sorted)
      {
        iCompareCount++;
        // Compare
        if (sortedSet.key == fcsKey2)
          iCompareEquals++;
        // FcsBTreeN verze 2
        if (btnTest.BtnFastNext(ref fcsKey2, out fcsValue2, 2) == null)
          break;
      }
      Console.WriteLine($"count FcsFastBTreeN:  {btnTest.BtnUsedKeys()}");
      Console.WriteLine($"count SortedSet:      {iCompareCount}");
      Console.WriteLine($"checking:             {iCompareEquals}");

      Console.WriteLine("--------------------------------------");
      Console.WriteLine("Key ENTER press.");
      Console.ReadLine();
    }
  }
}