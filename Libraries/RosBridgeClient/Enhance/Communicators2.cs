using System;

namespace RosSharp.RosBridgeClient
{
   public delegate void ServiceCallErrorHandler<T>(T t) where T : Exception;


   internal abstract class ServiceConsumer2
   {
      internal abstract string Id { get; }
      internal abstract string Service { get; }
      internal abstract void Consume(string message, ISerializer serializer);
      internal abstract void OnFault(Exception ex);
   }

   internal class ServiceConsumer2<Tin, Tout> : ServiceConsumer2 where Tin : Message where Tout : Message
   {
      internal override string Id { get; }
      internal override string Service { get; }


      internal ServiceResponseHandler<Tout> ServiceResponseHandler;
      internal ServiceCallErrorHandler<Exception> ServiceCallErrorHandler;

      internal ServiceConsumer2(string id, string service, ServiceResponseHandler<Tout> serviceResponseHandler,
         ServiceCallErrorHandler<Exception> serviceCallErrorHandler, out Communication serviceCall,
         Tin serviceArguments)
      {
         Id = id;
         Service = service;
         ServiceResponseHandler = serviceResponseHandler;
         ServiceCallErrorHandler = serviceCallErrorHandler;
         serviceCall = new ServiceCall<Tin>(id, service, serviceArguments);
      }


      internal override void Consume(string message, ISerializer serializer)
      {
         try
         {
            var deserialize = serializer.Deserialize<Tout>(message);
            ServiceResponseHandler.Invoke(deserialize);
         }
         catch (Exception e)
         {
            Console.WriteLine(e);
         }
      }

      internal override void OnFault(Exception ex)
      {
         ServiceCallErrorHandler.Invoke(ex);
      }
   }
}