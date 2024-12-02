# PinnedPool

Basically a copy of System.Buffers.SharedArrayPool<T> from the official 
dotnet 9.0 source tree, except
```csharp
buffer = GC.AllocateUninitializedArray<T>(minimumLength);
```
is changed into
```csharp
buffer = GC.AllocateUninitializedArray<T>(minimumLength, true);
```
Created as a stop-gap solution for native-interop and IO until coreclr
figures out how to integrate pinned-memory support for their array and memory
pools.

## Disclaimer
Though the code here is mostly adapted from the dotnet official repository,
and the correctness should be assumed to be good, this repository is not
affiliated to, nor have any official or personal connection with the official
dotnet project. 

Code and any artifacts created from this repository is shared with good 
intention with only minimal testing for basic functionality and does not 
resemble the code quality of any official dotnet project. Any issues related
to classes provided by this project should be posted in this repository.
