using System;
using System.Collections.Generic;
using System.Diagnostics;
using FriendlyCSharp.Databases;

namespace MultipleKeys.Multi.sample
{
  class Program
  {
    private class TestKV : FcsFastBTreeN<BtnKey, BtnValue>
    {
      protected override bool BtnUpdates(BtnKey keyAdd, BtnValue valueAdd, ref BtnValue valueUpdates, object objUpdates)
      {
        // Resize ?
        if (valueUpdates.aDateTime.Length <= valueUpdates.count)
        {
          if (valueUpdates.aDateTime.Length >= 1024)
            Array.Resize<DateTime>(ref valueUpdates.aDateTime, valueUpdates.aDateTime.Length + 256);
          else
            Array.Resize<DateTime>(ref valueUpdates.aDateTime, valueUpdates.aDateTime.Length * 2);
        }
        // Resize ?
        if (valueUpdates.aRand.Length <= valueUpdates.count)
        {
          if (valueUpdates.aRand.Length >= 1024)
            Array.Resize<uint>(ref valueUpdates.aRand, valueUpdates.aRand.Length + 256);
          else
            Array.Resize<uint>(ref valueUpdates.aRand, valueUpdates.aRand.Length * 2);
        }
        // Update
        if (valueAdd.aDateTime == null)
          valueUpdates.aDateTime[valueUpdates.count] = DateTime.MinValue;
        else
          valueUpdates.aDateTime[valueUpdates.count] = valueAdd.aDateTime[0];
        // Update
        if (valueAdd.aRand == null)
          valueUpdates.aRand[valueUpdates.count] = uint.MaxValue;
        else
          valueUpdates.aRand[valueUpdates.count] = valueAdd.aRand[0];
        valueUpdates.count++;
        return true;
      }
      //////////////////////////
      protected override void BtnAddFirst(BtnValue valueIn, out BtnValue valueAdd)
      {
        if (valueIn.count != 1)
        {
          valueIn.count = 1;
          valueIn.aDateTime = new DateTime[1];
          valueIn.aRand = new uint[1];
        }
        if (valueIn.aDateTime == null)
        {
          valueIn.aDateTime = new DateTime[1];
          valueIn.aDateTime[0] = DateTime.MinValue;
        }
        if (valueIn.aRand == null)
        {
          valueIn.aRand = new uint[1];
          valueIn.aRand[0] = uint.MaxValue;
        }
        base.BtnAddFirst(valueIn, out valueAdd);
      }
      //////////////////////////
      private void _BtnUsedValues(ref uint Count, BtnKeyValuePage QQ)
      {
        if (QQ != null)
        {
          if (QQ.kvPageNextRight != null)
          {
            _BtnUsedValues(ref Count, QQ.kvPageNextRight[0]);
            for (int II = 1; II <= QQ.iDataCount; II++)
            {
              _BtnUsedValues(ref Count, QQ.kvPageNextRight[II]);
              Count += (uint)QQ.aData[II].value.count;
            }
          }
          else
          {
            for (int II = 1; II <= QQ.iDataCount; II++)
              Count += (uint)QQ.aData[II].value.count;
          }
        }
      }
      //////////////////////////
      public uint BtnUsedValues()
      {
        uint Count = 0;
        _BtnUsedValues(ref Count, _btnRoot);
        return Count;
      }
      //////////////////////////
      public TestKV() : base(32, 32)
      {
      }
    }
    //
    private struct BtnKey : IComparable<BtnKey>
    {
      public string keyCity;
      public long keyId;
      //public DateTime key0;
      //public string key1;
      //public long key2;
      public int CompareTo(BtnKey keyY)
      {
        // return < 0 (less), = 0 (equal), > 0 (greater)
        int iResult = String.Compare(keyCity, keyY.keyCity, StringComparison.Ordinal);
        if (iResult == 0)
        {
          iResult = keyId.CompareTo(keyY.keyId);
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
    }
    //
    private struct BtnValue
    {
      public int count;
      public DateTime[] aDateTime;
      public uint[] aRand;
    }
    //
    //
    //
    static void Main(string[] args)
    {
      Console.OutputEncoding = System.Text.Encoding.UTF8;
      Console.WriteLine(String.Format("MultipleKeys.Core11.sample, {0}", (IntPtr.Size == 4) ? "32 bit" : "64 bit"));
      Console.WriteLine("----------------------------------");

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
      Console.WriteLine("----------------------------------");
      Console.WriteLine("FcsFastBTreeN");
      TestKV btnTest = new TestKV();
      var swFcsKV = Stopwatch.StartNew();
      for (int iter = 0; iter < 100; iter++)
      {
        for (int idx = 0; idx < max; idx++)
        {
          BtnKey key;
          key.keyCity = aCity[idx % aCity.Length];
          key.keyId = idx % 1200;
          BtnValue value;
          value.count = 1;
          if (idx % 50 == 12)
            value.aRand = null;
          else
          {
            value.aRand = new uint[1];
            value.aRand[0] = aRand[idx];
          }
          if (idx % 60 == 12)
            value.aDateTime = null;
          else
          {
            value.aDateTime = new DateTime[1];
            value.aDateTime[0] = DateTime.Now;
          }
          if (btnTest.BtnAdd(key, ref value, null) != null)
            iPocetAdd++;
        }
      }
      swFcsKV.Stop();
      iMem = GC.GetTotalMemory(true);
      Console.WriteLine("UsedMemory {0,5} MB [{1,5:N1} s] | {3} keys | Δ {2,3} MB | {4,8:N0} ns   | {5,10:N0} values", iMem >> 20, swFcsKV.Elapsed.TotalSeconds, (iMem - iMemOld) >> 20,
                        btnTest.BtnUsedKeys(), ((double)(swFcsKV.Elapsed.TotalMilliseconds * 1000000) / iPocetAdd), btnTest.BtnUsedValues());
      iMemOld = iMem;
      int iCompareCount = 0;
      swFcsKV.Restart();
      foreach (KeyValuePair<BtnKey, BtnValue>? value in btnTest)
        iCompareCount++;
      swFcsKV.Stop();
      Console.WriteLine("\nFcsFastBTreeN - foreach()");
      Console.WriteLine($"{((double)(swFcsKV.Elapsed.TotalMilliseconds * 1000000) / iCompareCount),9:N2} ns  [{swFcsKV.Elapsed.TotalMilliseconds,11} ms | {iCompareCount} keys ]{iCompareCount / swFcsKV.Elapsed.TotalSeconds,20:N0} IOPS");

      iCompareCount = 0;
      FcsBTreeN<BtnKey, BtnValue>.BtnEnumerator btnEn = btnTest.GetEnumeratorEx(false);
      swFcsKV.Restart();
      while (btnEn.MoveNext())
      {
        KeyValuePair<BtnKey, BtnValue>? value = btnEn.Current;
        iCompareCount++;
      }
      swFcsKV.Stop();
      Console.WriteLine("\nFcsBTreeN - foreach()");
      Console.WriteLine($"{((double)(swFcsKV.Elapsed.TotalMilliseconds * 1000000) / iCompareCount),9:N2} ns  [{swFcsKV.Elapsed.TotalMilliseconds,11} ms | {iCompareCount} keys ]{iCompareCount / swFcsKV.Elapsed.TotalSeconds,20:N0} IOPS");

      iCompareCount = 0;
      swFcsKV.Restart();
      if (btnTest.BtnFastFirst(out BtnKey fcsKey2, out BtnValue fcsValue2, 2) != null)
      {
        iCompareCount++;
        while (btnTest.BtnFastNext(ref fcsKey2, out fcsValue2, 2) != null)
          iCompareCount++;
      }
      swFcsKV.Stop();
      Console.WriteLine("\nFcsFastBTreeN - BtnFastFirst()/BtnFastNext()");
      Console.WriteLine($"{((double)(swFcsKV.Elapsed.TotalMilliseconds * 1000000) / iCompareCount),9:N2} ns  [{swFcsKV.Elapsed.TotalMilliseconds,11} ms | {iCompareCount} keys ]{iCompareCount / swFcsKV.Elapsed.TotalSeconds,20:N0} IOPS");
      //
      //

      Console.WriteLine("----------------------------------");
      Console.WriteLine("Key ENTER press.");
      Console.ReadLine();
    }
  }
}