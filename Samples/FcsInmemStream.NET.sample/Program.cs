using System;
using System.Diagnostics;
using System.Runtime.InteropServices;
using FriendlyCSharp.Databases;

namespace FcsInmemStream.NET.sample
{
  class Program
  {
    public unsafe struct StructIms
    {
      public int key;
      public uint value;
      //
      public DateTime dt;
      //
      public Guid guid;
      //
      public fixed byte buff[32]; // 4064, 992, 480, 224, 96, 32
    }

    static void Main(string[] args)
    {
      Console.OutputEncoding = System.Text.Encoding.UTF8;
      Console.WriteLine(String.Format("FcsInmemStream.NET452.sample, {0}", (IntPtr.Size == 4) ? "32 bit" : "64 bit"));
      Console.WriteLine("------------------------------------");

      int iSizeT = Marshal.SizeOf(default(StructIms));
      int iRepeat = 6000;
      if (iSizeT > 128)
        iRepeat = 4000;
      else if (iSizeT > 1024)
        iRepeat = 1000;
      int iID = 0;
      int cacheCount = 1000;
      StructIms[] aIms = new StructIms[cacheCount];
      FcsInmemStream<StructIms>.ImsEnumeratorCacheLen = 128;
      FcsInmemStream<StructIms> ims = FcsInmemStream<StructIms>.Open(2);
#if DEBUG
      ims.FuncException = true;
#else
      ims.FuncException = false;
#endif
      Stopwatch swX = new Stopwatch();
      for (int te = 0; te < iRepeat; te++)
      {
        for (int ui = 0; ui < cacheCount; ui++)
        {
          aIms[ui].key = iID++;
          aIms[ui].value = 0;
          aIms[ui].dt = DateTime.Now;
          aIms[ui].guid = Guid.NewGuid();
        }
        swX.Start();
        ims.Append(aIms);
        swX.Stop();
      }
      Console.WriteLine($"Records:  {ims.Length} | size: {iSizeT} Byte");
      Console.WriteLine("\nAppend IOPS:  {0,13:N0} [{1:N7} s] | count: {2,10:N0}", iID / swX.Elapsed.TotalSeconds, swX.Elapsed.TotalSeconds, iID);

      // foreach()
      iID = 0;
      foreach (StructIms value in ims)
      {
        if (value.key != iID)
          break;
        iID++;
      }
      iID = 0;
      swX.Reset();
      swX.Start();
      foreach (StructIms value in ims)
      {
        if (value.key != iID)
          break;
        iID++;
      }
      swX.Stop();
      Console.WriteLine("foreach IOPS: {0,13:N0} [{1:N7} s] | count: {2,10:N0}", iID / swX.Elapsed.TotalSeconds, swX.Elapsed.TotalSeconds, iID);

      // Write()
      long pos = 0;
      while (pos < ims.Length)
      {
        int iRead = ims.Read(pos, aIms, (UInt16)cacheCount);
        pos += iRead;
      }
      Array.Clear(aIms, 0, aIms.Length);
      iID = 0;
      pos = 0;
      swX.Reset();
      while (pos < ims.Length)
      {
        ims.Read(pos, aIms, (UInt16)cacheCount);
        for (int ui = 0; ui < cacheCount; ui++)
        {
          aIms[ui].key *= 10;
          iID++;
        }
        swX.Start();
        ims.Write(pos, aIms, 0, (UInt16)cacheCount);
        swX.Stop();
        pos += cacheCount;
      }
      Console.WriteLine("Write IOPS:   {0,13:N0} [{1:N7} s] | count: {2,10:N0}", iID / swX.Elapsed.TotalSeconds, swX.Elapsed.TotalSeconds, iID);

      // Read()
      Array.Clear(aIms, 0, aIms.Length);
      iID = 0;
      pos = 0;
      swX.Reset();
      while (pos < ims.Length)
      {
        swX.Start();
        int iRead = ims.Read(pos, aIms, (UInt16)cacheCount);
        swX.Stop();
        pos += iRead;
        for (int ui = 0; ui < iRead; ui++)
        {
          if (aIms[ui].key != iID)
            break;
          iID += 10;
        }
      }
      iID /= 10;
      Console.WriteLine("Read IOPS:    {0,13:N0} [{1:N7} s] | count: {2,10:N0}", iID / swX.Elapsed.TotalSeconds, swX.Elapsed.TotalSeconds, iID);
      ims.Close();

      Console.WriteLine("------------------------------------");
      Console.WriteLine("Key ENTER press.");
      Console.ReadLine();
    }
  }
}
