// 20231205 uar-hyg


using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using RosSharp.RosBridgeClient.Protocols;

namespace RosSharp.RosBridgeClient
{
   public class RosSocket2
   {
      public IProtocol protocol;

      public enum SerializerEnum
      {
         Microsoft,
         Newtonsoft_JSON,
         Newtonsoft_BSON
      }

      private Dictionary<string, Publisher> Publishers = new Dictionary<string, Publisher>();
      private Dictionary<string, Subscriber> Subscribers = new Dictionary<string, Subscriber>();
      private Dictionary<string, ServiceProvider> ServiceProviders = new Dictionary<string, ServiceProvider>();
      private Dictionary<string, ServiceConsumer2> ServiceConsumers = new Dictionary<string, ServiceConsumer2>();
      private ISerializer Serializer;
      private object SubscriberLock = new object();

      public RosSocket2(IProtocol protocol, SerializerEnum serializer = SerializerEnum.Microsoft)
      {
         this.protocol = protocol;
         switch (serializer)
         {
            case SerializerEnum.Microsoft:
            {
               Serializer = new MicrosoftSerializer();
               break;
            }
            case SerializerEnum.Newtonsoft_JSON:
            {
               Serializer = new NewtonsoftJsonSerializer();
               break;
            }
            case SerializerEnum.Newtonsoft_BSON:
            {
               Serializer = new NewtonsoftBsonSerializer();
               break;
            }
         }

         this.protocol.OnReceive += (sender, e) => Receive(sender, e);
         this.protocol.Connect();
      }

      public void Close(int millisecondsWait = 0)
      {
         bool isAnyCommunicatorActive = Publishers.Count > 0 || Subscribers.Count > 0 || ServiceProviders.Count > 0;

         while (Publishers.Count > 0)
            Unadvertise(Publishers.First().Key);

         while (Subscribers.Count > 0)
            Unsubscribe(Subscribers.First().Key);

         while (ServiceProviders.Count > 0)
            UnadvertiseService(ServiceProviders.First().Key);

         // Service consumers do not stay on. So nothing to unsubscribe/unadvertise

         if (isAnyCommunicatorActive)
         {
            Thread.Sleep(millisecondsWait);
         }

         protocol.Close();
      }

      #region Publishers

      public string Advertise<T>(string topic) where T : Message
      {
         string id = topic;
         if (Publishers.ContainsKey(id))
            Unadvertise(id);

         Publishers.Add(id, new Publisher<T>(id, topic, out Advertisement advertisement));
         Send(advertisement);
         return id;
      }

      public void Publish(string id, Message message)
      {
         Send(Publishers[id].Publish(message));
      }

      public void Unadvertise(string id)
      {
         Send(Publishers[id].Unadvertise());
         Publishers.Remove(id);
      }

      #endregion

      #region Subscribers

      public string Subscribe<T>(string topic, SubscriptionHandler<T> subscriptionHandler, int throttle_rate = 0,
         int queue_length = 1, int fragment_size = int.MaxValue, string compression = "none") where T : Message
      {
         string id;
         lock (SubscriberLock)
         {
            id = GetUnusedCounterID(Subscribers, topic);
            Subscription subscription;
            Subscribers.Add(id,
               new Subscriber<T>(id, topic, subscriptionHandler, out subscription, throttle_rate, queue_length,
                  fragment_size, compression));
            Send(subscription);
         }

         return id;
      }

      public void Unsubscribe(string id)
      {
         Send(Subscribers[id].Unsubscribe());
         Subscribers.Remove(id);
      }

      #endregion

      #region ServiceProviders

      public string AdvertiseService<Tin, Tout>(string service, ServiceCallHandler<Tin, Tout> serviceCallHandler)
         where Tin : Message where Tout : Message
      {
         string id = service;
         if (ServiceProviders.ContainsKey(id))
            UnadvertiseService(id);

         ServiceAdvertisement serviceAdvertisement;
         ServiceProviders.Add(id,
            new ServiceProvider<Tin, Tout>(service, serviceCallHandler, out serviceAdvertisement));
         Send(serviceAdvertisement);
         return id;
      }

      public void UnadvertiseService(string id)
      {
         Send(ServiceProviders[id].UnadvertiseService());
         ServiceProviders.Remove(id);
      }

      #endregion

      #region ServiceConsumers

      public string CallService2<Tin, Tout>(string service, ServiceResponseHandler<Tout> serviceResponseHandler
         , ServiceCallErrorHandler<Exception> serviceCallErrorHandler, Tin serviceArguments)
         where Tin : Message where Tout : Message
      {
         string id = GetUnusedCounterID(ServiceConsumers, service);
         Communication serviceCall;
         ServiceConsumers.Add(id,
            new ServiceConsumer2<Tin, Tout>(id, service, serviceResponseHandler, serviceCallErrorHandler,
               out serviceCall, serviceArguments));
         Send(serviceCall);
         return id;
      }

      #endregion

      private void Send<T>(T communication) where T : Communication
      {
         protocol.Send(Serializer.Serialize<T>(communication));
         return;
      }

      private void Receive(object sender, EventArgs e)
      {
         byte[] buffer = ((MessageEventArgs)e).RawData;
         DeserializedObject jsonElement = Serializer.Deserialize(buffer);

         switch (jsonElement.GetProperty("op"))
         {
            case "publish":
            {
               string topic = jsonElement.GetProperty("topic");
               string msg = jsonElement.GetProperty("msg");
               foreach (Subscriber subscriber in SubscribersOf(topic))
                  subscriber.Receive(msg, Serializer);
               return;
            }
            case "service_response":
            {
               string id = jsonElement.GetProperty("id");
               try
               {
                  var values = jsonElement.GetProperty("values");
                  var isSuccess = bool.Parse(jsonElement.GetProperty("result"));
                  if (isSuccess)
                     ServiceConsumers[id].Consume(values, Serializer);
                  else
                     ServiceConsumers[id].OnFault(new Exception(values));
               }
               finally
               {
                  ServiceConsumers.Remove(id);
               }

               return;
            }
            case "call_service":
            {
               string id = jsonElement.GetProperty("id");
               string service = jsonElement.GetProperty("service");
               string args = jsonElement.GetProperty("args");
               Send(ServiceProviders[service].Respond(id, args, Serializer));
               return;
            }
         }
      }

      private List<Subscriber> SubscribersOf(string topic)
      {
         return Subscribers.Where(pair => pair.Key.StartsWith(topic + ":")).Select(pair => pair.Value).ToList();
      }

      private static string GetUnusedCounterID<T>(Dictionary<string, T> dictionary, string name)
      {
         int I = 0;
         string id;
         do
            id = name + ":" + I++;
         while (dictionary.ContainsKey(id));
         return id;
      }
   }
}